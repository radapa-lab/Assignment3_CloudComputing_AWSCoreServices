import boto3
import time
from flask import Flask

# --- CONFIGURATION ---
AWS_REGION = "us-east-1"
ATHENA_DATABASE = "orders_db"
S3_OUTPUT_LOCATION = "s3://itcs6190-assignment3-rishitha/enriched/"
# ----------------------

app = Flask(__name__)

athena_client = boto3.client('athena', region_name=AWS_REGION)

queries_to_run = [

    {
        "title": "1. Total Sales by Customer",
        "query": """
            SELECT customer, SUM(amount) AS total_sales
            FROM processed
            GROUP BY customer
            ORDER BY total_sales DESC;
        """
    },

    {
        "title": "2. Monthly Order Volume and Revenue",
        "query": """
            SELECT
                DATE_FORMAT(CAST(orderdate AS DATE), '%Y-%m') AS month,
                COUNT(*) AS total_orders,
                SUM(amount) AS total_revenue
            FROM processed
            GROUP BY DATE_FORMAT(CAST(orderdate AS DATE), '%Y-%m')
            ORDER BY month;
        """
    },
    {
        "title": "3. Order Status Dashboard",
        "query": """
            SELECT
                status,
                COUNT(*) AS total_orders
            FROM processed
            GROUP BY status
            ORDER BY total_orders DESC;
        """
    },

    {
        "title": "4. Average Order Value (AOV) per Customer",
        "query": """
            SELECT
                customer,
                AVG(amount) AS avg_order_value
            FROM processed
            GROUP BY customer
            ORDER BY avg_order_value DESC;
        """
    },

    {
        "title": "5. Top 10 Largest Orders in February 2025",
        "query": """
            SELECT *
            FROM processed
            WHERE DATE_FORMAT(CAST(orderdate AS DATE), '%Y-%m') = '2025-02'
            ORDER BY amount DESC
            LIMIT 10;
        """
    }
]


def run_athena_query(query):
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION}
        )

        query_execution_id = response['QueryExecutionId']

        while True:
            status = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )['QueryExecution']['Status']['State']

            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

            time.sleep(1)

        if status == 'SUCCEEDED':
            result = athena_client.get_query_results(QueryExecutionId=query_execution_id)

            header = [col['Label'] for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]

            rows = [
                [field.get('VarCharValue', '') for field in row['Data']]
                for row in result['ResultSet']['Rows'][1:]
            ]

            return header, rows
        else:
            return None, "Query failed"

    except Exception as e:
        return None, str(e)


@app.route('/')
def index():
    html = "<html><head><title>Athena Dashboard</title></head><body>"
    html += "<h1>📊 Athena Orders Dashboard</h1>"

    for item in queries_to_run:
        html += f"<h2>{item['title']}</h2>"

        header, results = run_athena_query(item['query'])

        if header:
            html += "<table border='1' cellpadding='5'>"
            html += "<tr>" + "".join([f"<th>{h}</th>" for h in header]) + "</tr>"

            for row in results:
                html += "<tr>" + "".join([f"<td>{cell}</td>" for cell in row]) + "</tr>"

            html += "</table><br>"
        else:
            html += f"<p>Error: {results}</p>"

    html += "</body></html>"
    return html


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
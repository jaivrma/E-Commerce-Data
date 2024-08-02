import sqlite3
import pandas as pd
import requests
from io import StringIO

# Download the CSV file
csv_url = 'https://github.com/jaivrma/E-Commerce-Data/raw/main/data.csv'
response = requests.get(csv_url)
csv_data = StringIO(response.text)

# Load the CSV file into a Pandas DataFrame
df = pd.read_csv(csv_data)

# Connect to SQLite database (create a new database if it doesn't exist)
conn = sqlite3.connect('ecommerce_data.db')
cursor = conn.cursor()

# Create table query
create_table_query = '''
CREATE TABLE IF NOT EXISTS ecommerce_data (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INTEGER,
    InvoiceDate TEXT,
    UnitPrice REAL,
    CustomerID TEXT,
    Country TEXT
);
'''
cursor.execute(create_table_query)
conn.commit()

# Insert data into the table
df.to_sql('ecommerce_data', conn, if_exists='append', index=False)

# SQL queries to analyze user engagement
# Query to get the total number of orders per country
total_orders_query = '''
SELECT Country, COUNT(DISTINCT InvoiceNo) AS TotalOrders
FROM ecommerce_data
GROUP BY Country
ORDER BY TotalOrders DESC;
'''

# Query to get the total quantity sold per customer
total_quantity_per_customer_query = '''
SELECT CustomerID, SUM(Quantity) AS TotalQuantity
FROM ecommerce_data
GROUP BY CustomerID
ORDER BY TotalQuantity DESC;
'''

# Query to get the total revenue per country
total_revenue_query = '''
SELECT Country, SUM(Quantity * UnitPrice) AS TotalRevenue
FROM ecommerce_data
GROUP BY Country
ORDER BY TotalRevenue DESC;
'''

# Query to get the average order value per country
avg_order_value_query = '''
SELECT Country, AVG(Quantity * UnitPrice) AS AvgOrderValue
FROM ecommerce_data
GROUP BY Country
ORDER BY AvgOrderValue DESC;
'''

# Query to find the top 5 products by total quantity sold
top_products_query = '''
SELECT Description, SUM(Quantity) AS TotalQuantitySold
FROM ecommerce_data
GROUP BY Description
ORDER BY TotalQuantitySold DESC
LIMIT 5;
'''

# Execute queries and print results
total_orders = pd.read_sql_query(total_orders_query, conn)
total_quantity_per_customer = pd.read_sql_query(total_quantity_per_customer_query, conn)
total_revenue = pd.read_sql_query(total_revenue_query, conn)
avg_order_value = pd.read_sql_query(avg_order_value_query, conn)
top_products = pd.read_sql_query(top_products_query, conn)

print("Total Orders per Country:")
print(total_orders)
print("\nTotal Quantity Sold per Customer:")
print(total_quantity_per_customer)
print("\nTotal Revenue per Country:")
print(total_revenue)
print("\nAverage Order Value per Country:")
print(avg_order_value)
print("\nTop 5 Products by Total Quantity Sold:")
print(top_products)

# Close the database connection
conn.close()

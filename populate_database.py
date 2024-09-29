import pandas as pd
import sqlite3

conn = sqlite3.connect('shipment_database.db')  
cursor = conn.cursor()

spreadsheet_0 = pd.read_excel('shipping_data_0.csv')  
spreadsheet_1 = pd.read_excel('shipping_data_1.csv')  
spreadsheet_2 = pd.read_excel('shipping_data_2.csv')  

spreadsheet_0.to_sql('product', conn, if_exists='append', index=False)

cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipments (
        product_name TEXT,
        quantity INTEGER,
        origin TEXT,
        destination TEXT
    );
''')

merged_data = pd.merge(spreadsheet_1, spreadsheet_2, on='shipping_identifier')

for index, row in merged_data.iterrows():
    product_name = row['product']
    quantity = row['product_quantity']
    origin = row['origin_warehouse']
    destination = row['destination_store']

    cursor.execute('''
        INSERT INTO table_name_1 (product_name, quantity, origin, destination)
        VALUES (?, ?, ?, ?)
    ''', (product_name, quantity, origin, destination))

conn.commit()
conn.close()
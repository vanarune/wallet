from flask import Flask, render_template
import pandas as pd

import pandas as pd
import os, uuid, sys
from azure.storage.blob import BlockBlobService
from tabula import read_pdf
import numpy as np

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('mode.chained_assignment', None)

block_blob_service = BlockBlobService(account_name='logandb20191009', account_key='nOkaHZmzjyshT8kHbIoKsNDbDEjah4YIoEzg11+aAZAaFNEBXrKFp9wftQuWxZ/V2C2KcuuPcFqeBjUrqW6GkA==')
container_name ='logan-db'
generator = block_blob_service.list_blobs(container_name)

for blob in generator:
    print(blob.name)
    print("{}".format(blob.name))
    #check if the path contains a folder structure, create the folder structure
    if "/" in "{}".format(blob.name):
        print("there is a path in this")
        #extract the folder path and check if that folder exists locally, and if not create it
        head, tail = os.path.split("{}".format(blob.name))
        print(head)
        print(tail)
        if (os.path.isdir(os.getcwd()+ "/" + head)):
            #download the files to this directory
            print("directory and sub directories exist")
            block_blob_service.get_blob_to_path(container_name,blob.name,os.getcwd()+ "/" + head + "/" + tail)
            if head=="Input" and tail.lower().endswith(('.csv')):
                input_data = pd.read_csv(os.getcwd() + "/" + head + "/" + tail, sep=',', error_bad_lines=False,
                                         index_col=False, dtype='unicode')
            elif head=="lookup" and tail=="co_emission.csv":
                lookup_emission_data = pd.read_csv(os.getcwd() + "/" + head + "/" + tail, sep=',', error_bad_lines=False,
                                         index_col=False, dtype='unicode')
            elif head=="lookup" and tail=="cust_fleet_map.csv":
                lookup_fleet_map_data = pd.read_csv(os.getcwd() + "/" + head + "/" + tail, sep=',', error_bad_lines=False,
                                         index_col=False, dtype='unicode')
            elif head == "lookup" and tail.lower().endswith(('.pdf')):
                lookup_rodata_pdf = read_pdf(os.getcwd() + "/" + head + "/" + tail)
        else:
            #create the diretcory and download the file to it
            print("directory doesn't exist, creating it now")
            os.makedirs(os.getcwd()+ "/" + head, exist_ok=True)
            print("directory created, download initiated")
            block_blob_service.get_blob_to_path(container_name,blob.name,os.getcwd()+ "/" + head + "/" + tail)
            if head=="Input" and tail.lower().endswith(('.csv')):
                input_data = pd.read_csv(os.getcwd() + "/" + head + "/" + tail, sep=',', error_bad_lines=False,
                                         index_col=False, dtype='unicode')
            elif head=="lookup" and tail=="co_emission.csv":
                lookup_emission_data = pd.read_csv(os.getcwd() + "/" + head + "/" + tail, sep=',', error_bad_lines=False,
                                         index_col=False, dtype='unicode')
            elif head=="lookup" and tail=="cust_fleet_map.csv":
                lookup_fleet_map_data = pd.read_csv(os.getcwd() + "/" + head + "/" + tail, sep=',', error_bad_lines=False,
                                         index_col=False, dtype='unicode')
            elif head == "lookup" and tail.lower().endswith(('.pdf')):
                lookup_rodata_pdf = read_pdf(os.getcwd() + "/" + head + "/" + tail)

    else:
        block_blob_service.get_blob_to_path(container_name,blob.name,blob.name)

input_fleet_map_data = pd.merge(input_data, lookup_fleet_map_data, left_on='Customer_ID', right_on='cust_id')

final_input_data=pd.merge(input_fleet_map_data, lookup_emission_data, left_on='type', right_on='Type')

#print(final_input_data.head(2).to_string(index=False))

lookup_rodata_pdf = lookup_rodata_pdf.rename({'Name of dealership': 'dealer'}, axis=1)
#print(lookup_rodata_pdf.head(2).to_string(index=False))

final_input_data['dealer'] = final_input_data['Transaction_Details'].str.extract('([a-zA-Z ]+)', expand=False).str.strip()
#print(final_input_data.head(2).to_string(index=False))

out_in_data=pd.merge(final_input_data, lookup_rodata_pdf, on='dealer', how='left')
#print(out_in_data.to_string(index=False))

out_in_data['category']=np.where(out_in_data['State'].isnull(), 'OTHERS', 'PETROL')
#print(out_in_data.to_string(index=False))

out_results=out_in_data[out_in_data['category']=='PETROL']
print(out_results.to_string(index=False))

out_results['co2_emmision_score'] = out_results['CO2_Ltr'].astype(float) * out_results['Amount'].astype(float) / 75

#print(out_results)

out_results['points'] = np.select(
    [
        out_results['co2_emmision_score'].between(0, 2, inclusive=True),
        out_results['co2_emmision_score'].between(3, 5, inclusive=True),
        out_results['co2_emmision_score'].between(6, 8, inclusive=True),
        out_results['co2_emmision_score'].between(9, 10, inclusive=True),
        out_results['co2_emmision_score'].between(11, 25, inclusive=True),
        out_results['co2_emmision_score'].between(26, 50, inclusive=True),
        out_results['co2_emmision_score'].between(51, 100, inclusive=True),
        out_results['co2_emmision_score'].between(101, 200, inclusive=True),
        out_results['co2_emmision_score'].between(201, 300, inclusive=True)
    ],
    [
        100,
        80,
        60,
        40,
        35,
        30,
        25,
        20,
        10
    ],
    default=0
)
df = out_results

#print(out_results)

length = len(df)
app = Flask(__name__)


@app.route("/")
def home():
    return 'Hey its Python Flask application123!'
    #return render_template("index.html.j2")


@app.route("/df")
def dataframe():
    return render_template("df.html.j2", length=length, dataframe=df.to_html())


@app.route("/dfcustom")
def dfcustom():
    data = df.to_dict(orient="records")
    headers = df.columns
    print(headers)
    return render_template("dfcustom.html.j2", data=data, headers=headers)


if __name__ == "__main__":
    app.run(debug=True, port=5000)

import urllib.request
import csv
import ssl
import json
import time
import gspread

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly']

def get_data():
    urlpage =  "https://www.jisilu.cn/data/etf/etf_list/?___jsl=LST___t={0}&rp=25&page=1".format(int(time.time()))
    result = urllib.request.urlopen(urlpage, context=ssl.SSLContext()).read()
    rows = json.loads(result)['rows']
    result = {}
    for row in rows:
        cell = row['cell']
        result[cell['fund_id']] = {'id': cell['fund_id'], 'price': cell['price'], 'name': cell['fund_nm']}
    return result

def update_existing_data(sheet, existing_ids, new_data_dict):
    update_data = []
    for id in existing_ids:
        try:
           new_data = new_data_dict[id]
           update_data.append([new_data['price']])
        except KeyError:
            print("can not find existing id %s" % id)
            update_data.append([None])
            continue
    sheet.update('E3:E1000', update_data, raw=False)

def insert_new_data(sheet, existing_ids, new_data_dict):
    new_data_ids = list(new_data_dict)
    to_be_insert_ids = [id for id in new_data_ids if id not in existing_ids]
    values = [[None, None, new_data_dict[id]['name'], id, new_data_dict[id]['price']] for id in to_be_insert_ids]
    sheet.insert_rows(values, len(existing_ids) + 3, value_input_option='USER_ENTERED')


def main():
    gc = gspread.oauth(scopes=SCOPES)
    sheets = gc.open("ETF")
    sheet = sheets.worksheet("Changying")
    all_values = sheet.get("D3:D1000")
    existing_ids = [row[0] for row in all_values if len(row) > 0]
    data_dict = get_data()
    update_existing_data(sheet, existing_ids, data_dict)
    #insert_new_data(sheet, existing_ids, data_dict)

if __name__ == '__main__':
    main()

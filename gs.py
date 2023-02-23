import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

import psycopg2
from psycopg2 import Error, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, AsIs
import sqlalchemy as sa

from sqlalchemy import create_engine
from pathlib import Path

import datetime
import schedule
import time


datetimes = datetime.datetime.today().strftime("%Y-%m-%d-%H.%M") 

def parser():
    LINK = ("https://marketcap.ru/stocks/all-exchanges/russia/all-industries?paginate=250")

    def main():
        link = LINK
        print(get_data(get_html(link)))
    
    def get_html(link):
        response = requests.get(link)
        return response.text

    def get_data(html):
        global price

        soup = BeautifulSoup(html,'lxml')
        trs = soup.find("div", class_="row mt-5 ml-md-5 mr-md-5 ml-sm-0 mr-sm-0")
        #print(trs)
      
        tr = soup.find_all("tr")
        #print(tr)
    
        td = soup.find_all("td")
        #print(td)

        ###Извлекаем название компаний
        name = re.findall(r'<a href="/stocks/(\w*)">' , str(td))            
        #print (name)
    
        ###Извлекаем стоимость акции
        price = re.findall(r'<td>(\S*\s*\S*|\S*\s*\S*\s*\S*)</td>',str(td))
        #print(price)
    
         ###Извлекаем капитализацию компании на Российком рынке
        capital = re.findall(r'</td>, <td class="text-center"><p class="d-none">(\w*)</p>(\W*\s*\d*\s*\S*)\S*\s*\S*\s*\S*</td>,', str(td))
        #print (capital)

        #записываем данные в csv
        a = {'name':name,'capital': capital}
        df = pd.DataFrame.from_dict(a, orient='columns',)
        df.to_csv(r'D:\Users\cherk\OneDrive\Рабочий стол\Works\neuros\res.csv', sep=",", index=False)

        b = {'price':price}
        df = pd.DataFrame.from_dict(b, orient='columns',)
        #df.columns = df.columns.str.replace('price', datetimes)
        df.to_csv(r'D:\Users\cherk\OneDrive\Рабочий стол\Works\neuros\price.csv', sep=",", index=False, encoding="UTF-8")

        print(f'Данные за {datetimes} спарсены')

    if __name__=='__main__':
        main()

def get_sql():
    try:    
        # Подключение к существующей базе данных
        conn = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password = "amazingwar1",
                                      host = "127.0.0.1",
                                      port="5432")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        # Курсор для выполнения операций с базой данных
        cursor = conn.cursor()

        table = """CREATE TABLE mari (id SERIAL PRIMARY KEY , name VARCHAR, capital varchar)"""
        tableprice = "CREATE TABLE LICH (ID SERIAL PRIMARY KEY, PRICE VARCHAR, timestamp timestamp default current_timestamp)"
        #cursor.execute(table)
        #cursor.execute(tableprice)

        # Распечатать сведения о PostgreSQL
        print("Информация о сервере PostgreSQL")
        # Выполнение SQL-запроса
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("Вы подключены к - ", record, "\n")
        print(conn.get_dsn_parameters(), "\n")

        ##добавление названия и капитализации в бд
        data= pd.read_csv(r'D:\Users\cherk\OneDrive\Рабочий стол\Works\neuros\res.csv', sep=',')
        data = data[["name", "capital"]]
        engine = create_engine("postgresql+psycopg2://postgres:amazingwar1@127.0.0.1:5432/postgres")
        #data.to_sql('mari', engine , if_exists='append', index=False)
        
        #добавляем цены в таблицу postgresql
        query = ("INSERT INTO LICH (price) VALUES (%s) returning price")
        cursor.execute(query, (price,))                  
    
        for i in cursor.fetchall():
                print(i)

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print(f'Данные за {datetimes} записаны в PostgreSQL')     

#1
schedule.every().day.at("10:05").do(parser)
schedule.every().day.at("10:05").do(get_sql)
#2
schedule.every().day.at("11:00").do(parser)
schedule.every().day.at("11:00").do(get_sql)
#3
schedule.every().day.at("12:00").do(parser)
schedule.every().day.at("12:00").do(get_sql)
#4
schedule.every().day.at("13:00").do(parser)
schedule.every().day.at("13:00").do(get_sql)
#5
schedule.every().day.at("14:00").do(parser)
schedule.every().day.at("14:00").do(get_sql)
#6
schedule.every().day.at("15:00").do(parser)
schedule.every().day.at("15:00").do(get_sql)
#7
schedule.every().day.at("16:00").do(parser)
schedule.every().day.at("16:00").do(get_sql)
#8
schedule.every().day.at("17:30").do(parser)
schedule.every().day.at("17:00").do(get_sql)
#9
schedule.every().day.at("18:00").do(parser)
schedule.every().day.at("18:00").do(get_sql)
#10
schedule.every().day.at("18:40").do(parser)
schedule.every().day.at("18:40").do(get_sql)


while True:
    schedule.run_pending()
    time.sleep(1)

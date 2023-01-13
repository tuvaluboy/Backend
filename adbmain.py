from flask import Flask
from flask_restful import Api, Resource, reqparse, abort, fields, marshal_with
import json

#from flask_sqlalchemy import SQLAlchemy
import psycopg2
from psycopg2.extras import RealDictCursor


#-----------Queries ------------------------

GLOBAL_XMAS = """SELECT c.name, COUNT(xmas.customer_id) as total_orders
from public.world_administrative_boundaries AS c
JOIN public.xmassorder2011 AS xmas
ON ST_Intersects(xmas.geom,c.geom)
GROUP BY c.name
ORDER BY total_orders DESC;"""

GLOBAL_CNY = """SELECT c.name, COUNT(cny.customer_id) as total_orders
from public.world_administrative_boundaries AS c
JOIN public.chinesenewyear AS cny
ON ST_Intersects(cny.geom,c.geom)
GROUP BY c.name
ORDER BY total_orders DESC;"""

TAIWAN_XMAS ="""SELECT c.countyname, COUNT(xmass.customer_id) as total_orders
from public.county_moi_1090820 AS c
JOIN public.xmassorder2011 AS xmass
ON ST_Intersects(xmass.geom,c.geom)
GROUP BY c.countyname
ORDER BY total_orders DESC"""

TAIWAN_CNY = """SELECT c.countyname, COUNT(cny.customer_id) as total_orders
from public.county_moi_1090820 AS c
JOIN public.chinesenewyear AS cny
ON ST_Intersects(cny.geom,c.geom)
GROUP BY c.countyname
ORDER BY total_orders DESC"""

BIGGEST_ORDER_XMAS ="""SELECT customer_id, COUNT(order_id) AS ordertotal
FROM public.xmassorder2011
GROUP BY customer_id
ORDER BY ordertotal desc;"""

BIGGEST_ORDER_CNY ="""SELECT customer_id, COUNT(order_id) AS ordertotal
FROM public.chinesenewyear
GROUP BY customer_id
ORDER BY ordertotal desc;"""



con = psycopg2.connect("postgres://postgres:Tuvalu2022@localhost:5432/finalproject")
app = Flask(__name__)
api = Api(app)
   
@app.route("/api/globalcount")
def get_total_count():
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
           cursor.execute(GLOBAL_COUNTRY)
           data = cursor.fetchall()
           
           print(data)
           json_str = json.dumps(data) 
           return {"data": data}
       
@app.route("/api/biggestorderxmas")
def get_biggest_order_xmas():
    with con:
        with con.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(BIGGEST_ORDER_XMAS)
            data = cursor.fetchall()

            return{"data": data}


if __name__ == "__main__":
    app.run(debug=True)
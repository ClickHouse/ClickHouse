-- Azure Data Explore Test Data
-- let make_series_test_table = datatable (Supplier:string, Fruit:string, Price: real, Purchase:datetime)
-- [
-- 'Aldi','Apple',4,'2016-09-10',
-- 'Costco','Apple',2,'2016-09-11',
-- 'Aldi','Apple',6,'2016-09-10',
-- 'Costco','Snargaluff',100,'2016-09-12',
-- 'Aldi','Apple',7,'2016-09-12',
-- 'Aldi','Snargaluff',400,'2016-09-11',
-- 'Costco','Snargaluff',104,'2016-09-12',
-- 'Aldi','Apple',5,'2016-09-12',
-- 'Aldi','Snargaluff',600,'2016-09-11',
-- 'Costco','Snargaluff',200,'2016-09-10',
-- ];
DROP TABLE IF EXISTS make_series_test_table;
CREATE TABLE make_series_test_table
(    
   Supplier Nullable(String),
   Fruit String ,
   Price Float64,
   Purchase Date 
) ENGINE = Memory;
INSERT INTO make_series_test_table VALUES  ('Aldi','Apple',4,'2016-09-10'), ('Costco','Apple',2,'2016-09-11'), ('Aldi','Apple',6,'2016-09-10'), ('Costco','Snargaluff',100,'2016-09-12'), ('Aldi','Apple',7,'2016-09-12'), ('Aldi','Snargaluff',400,'2016-09-11'),('Costco','Snargaluff',104,'2016-09-12'),('Aldi','Apple',5,'2016-09-12'),('Aldi','Snargaluff',600,'2016-09-11'),('Costco','Snargaluff',200,'2016-09-10');
DROP TABLE IF EXISTS make_series_test_table2;
CREATE TABLE make_series_test_table2
(    
   Supplier Nullable(String),
   Fruit String ,
   Price Int32,
   Purchase Int32  
) ENGINE = Memory;
INSERT INTO make_series_test_table2 VALUES  ('Aldi','Apple',4,10),('Costco','Apple',2,11),('Aldi','Apple',6,10),('Costco','Snargaluff',100,12),('Aldi','Apple',7,12),('Aldi','Snargaluff',400,11),('Costco','Snargaluff',104,12),('Aldi','Apple',5,12),('Aldi','Snargaluff',600,11),('Costco','Snargaluff',200,10);
DROP TABLE IF EXISTS make_series_test_table3;
CREATE TABLE make_series_test_table3
(    
    timestamp datetime,
    metric Float64,
) ENGINE = Memory;
INSERT INTO make_series_test_table3 VALUES (parseDateTimeBestEffort('2016-12-31T06:00'), 50), (parseDateTimeBestEffort('2017-01-01'), 4), (parseDateTimeBestEffort('2017-01-02'), 3), (parseDateTimeBestEffort('2017-01-03'), 4), (parseDateTimeBestEffort('2017-01-03T03:00'), 6), (parseDateTimeBestEffort('2017-01-05'), 8), (parseDateTimeBestEffort('2017-01-05T13:40'), 13), (parseDateTimeBestEffort('2017-01-06'), 4), (parseDateTimeBestEffort('2017-01-07'), 3), (parseDateTimeBestEffort('2017-01-08'), 8), (parseDateTimeBestEffort('2017-01-08T21:00'), 8), (parseDateTimeBestEffort('2017-01-09'), 2), (parseDateTimeBestEffort('2017-01-09T12:00'), 11), (parseDateTimeBestEffort('2017-01-10T05:00'), 5);

set dialect = 'kusto';
print '-- from to';
make_series_test_table |  make-series PriceAvg = avg(Price) default=0 on Purchase from datetime(2016-09-10)  to datetime(2016-09-13) step 1d by Supplier, Fruit | order by Supplier, Fruit;
print '-- from';
make_series_test_table |  make-series PriceAvg = avg(Price) default=0 on Purchase from datetime(2016-09-10)  step 1d by Supplier, Fruit | order by Supplier, Fruit;
print '-- to';
make_series_test_table |  make-series PriceAvg = avg(Price) default=0 on Purchase to datetime(2016-09-13) step 1d by Supplier, Fruit | order by Supplier, Fruit;
print '-- without from/to';
make_series_test_table | make-series PriceAvg = avg(Price) default=0 on Purchase step 1d by Supplier, Fruit | order by Supplier, Fruit;
print '-- without by';
make_series_test_table | make-series PriceAvg = avg(Price) default=0 on Purchase step 1d;
print '-- without aggregation alias';
make_series_test_table | make-series avg(Price) default=0 on Purchase step 1d by Supplier, Fruit;
print '-- assign group alias';
make_series_test_table | make-series avg(Price) default=0 on Purchase step 1d by Supplier_Name = Supplier, Fruit;
print '-- 3d step';
make_series_test_table | make-series PriceAvg = avg(Price) default=0 on Purchase from datetime(2016-09-10)  to datetime(2016-09-13) step 3d by Supplier, Fruit | order by Supplier, Fruit;

print '-- numeric column'
print '-- from to';
make_series_test_table2 | make-series PriceAvg=avg(Price) default=0 on Purchase from 10 to  15 step  1.0  by Supplier, Fruit;
print '-- from';
make_series_test_table2 | make-series PriceAvg=avg(Price) default=0 on Purchase from 10 step  1.0  by Supplier, Fruit;
print '-- to';
make_series_test_table2 | make-series PriceAvg=avg(Price) default=0 on Purchase to 18 step  4.0  by Supplier, Fruit;
print '-- without from/to';
make_series_test_table2 | make-series PriceAvg=avg(Price) default=0 on Purchase step  2.0  by Supplier, Fruit;
print '-- without by';
make_series_test_table2 | make-series PriceAvg=avg(Price) default=0 on Purchase step  2.0;

make_series_test_table3 | make-series avg(metric) default=0  on timestamp from datetime(2017-01-01) to datetime(2017-01-10) step 1d 

-- print '-- summarize --'
-- make_series_test_table | summarize count() by format_datetime(bin(Purchase, 1d), 'yy-MM-dd');

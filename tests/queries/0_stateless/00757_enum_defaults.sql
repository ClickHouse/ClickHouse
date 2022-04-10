select os_name, count() from (SELECT CAST('iphone' AS Enum8('iphone' = 1, 'android' = 2)) AS os_name) group by os_name WITH TOTALS;
select toNullable(os_name) AS os_name, count() from (SELECT CAST('iphone' AS Enum8('iphone' = 1, 'android' = 2)) AS os_name) group by os_name WITH TOTALS;

DROP TABLE IF EXISTS auto_assgin_enum;
CREATE TABLE auto_assgin_enum (x enum('a', 'b')) ENGINE=MergeTree() order by x;
INSERT INTO auto_assgin_enum VALUES('a'), ('b');
select * from auto_assgin_enum;
DROP TABLE auto_assgin_enum;

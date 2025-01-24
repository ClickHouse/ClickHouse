DROP TABLE IF EXISTS decimal_dt;

CREATE TABLE decimal_dt (timestamp DateTime64(9)) ENGINE=MergeTree() ORDER BY timestamp;
INSERT INTO decimal_dt VALUES (toDate('2024-11-11')),(toDate('2024-11-12')),(toDate('2024-11-13')),(toDate('2024-11-14')),(toDate('2024-11-15')),(toDate('2024-11-16')),(toDate('2024-11-17'));
SELECT count() FROM decimal_dt WHERE toDayOfWeek(timestamp) > 3;

DROP TABLE IF EXISTS decimal_dt;

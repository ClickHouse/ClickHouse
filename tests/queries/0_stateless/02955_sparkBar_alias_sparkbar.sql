SET enable_analyzer = 1;
DROP TABLE IF EXISTS spark_bar_test;

CREATE TABLE spark_bar_test (`value` Int64, `event_date` Date) ENGINE = MergeTree ORDER BY event_date;

INSERT INTO spark_bar_test VALUES (1,'2020-01-01'), (3,'2020-01-02'), (4,'2020-01-02'), (-3,'2020-01-02'), (5,'2020-01-03'), (2,'2020-01-04'), (3,'2020-01-05'), (7,'2020-01-06'), (6,'2020-01-07'), (8,'2020-01-08'), (2,'2020-01-11');

SELECT sparkbar(9)(event_date,cnt) FROM (SELECT sum(value) as cnt, event_date FROM spark_bar_test GROUP BY event_date);
SELECT sparkBar(9)(event_date,cnt) FROM (SELECT sum(value) as cnt, event_date FROM spark_bar_test GROUP BY event_date);

DROP TABLE IF EXISTS spark_bar_test;

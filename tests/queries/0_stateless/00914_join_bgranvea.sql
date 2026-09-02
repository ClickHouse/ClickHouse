DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1 (A String, B String, ts DateTime) ENGINE = MergeTree PARTITION BY toStartOfDay(ts)  ORDER BY (ts, A, B);
CREATE TABLE table2 (B String, ts DateTime) ENGINE = MergeTree PARTITION BY toStartOfDay(ts) ORDER BY (ts, B);

insert into table1 values('a1','b1','2019-02-05 16:50:00'),('a1','b1','2019-02-05 16:55:00');
insert into table2 values('b1','2019-02-05 16:50:00'),('b1','2019-02-05 16:55:00');

SELECT t1.B, t2.B FROM table1 t1 ALL INNER JOIN table2 t2 ON t1.B = t2.B ORDER BY t1.B, t2.B;

DROP TABLE table1;
DROP TABLE table2;

CREATE TEMPORARY TABLE test (d Date, dt DateTime);
INSERT INTO test VALUES (toDateTime('2000-01-01 01:02:03'), toDate('2000-01-01'));
SELECT * FROM test;

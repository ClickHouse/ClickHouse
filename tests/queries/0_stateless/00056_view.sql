-- Tags: stateful
DROP TABLE IF EXISTS view;
CREATE VIEW view AS SELECT CounterID, count() AS c FROM test.hits GROUP BY CounterID;
SELECT count() FROM view;
SELECT c, count() FROM view GROUP BY c ORDER BY count() DESC LIMIT 10;
SELECT * FROM view ORDER BY c DESC LIMIT 10;
SELECT * FROM view SAMPLE 0.1 ORDER BY c DESC LIMIT 10;
DROP TABLE view;

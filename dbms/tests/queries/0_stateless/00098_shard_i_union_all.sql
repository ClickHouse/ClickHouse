DROP TABLE IF EXISTS test.report1;
DROP TABLE IF EXISTS test.report2;

CREATE TABLE test.report1(id UInt32, event_date Date, priority UInt32, description String) ENGINE = MergeTree(event_date, intHash32(id), (id, event_date, intHash32(id)), 8192);
CREATE TABLE test.report2(id UInt32, event_date Date, priority UInt32, description String) ENGINE = MergeTree(event_date, intHash32(id), (id, event_date, intHash32(id)), 8192);

INSERT INTO test.report1(id,event_date,priority,description) VALUES (1, '2015-01-01', 1, 'foo')(2, '2015-02-01', 2, 'bar')(3, '2015-03-01', 3, 'foo')(4, '2015-04-01', 4, 'bar')(5, '2015-05-01', 5, 'foo');
INSERT INTO test.report2(id,event_date,priority,description) VALUES (1, '2016-01-01', 6, 'bar')(2, '2016-02-01', 7, 'foo')(3, '2016-03-01', 8, 'bar')(4, '2016-04-01', 9, 'foo')(5, '2016-05-01', 10, 'bar');

SELECT * FROM (SELECT id, event_date, priority, description FROM remote('127.0.0.{2,3}', test, report1) UNION ALL SELECT id, event_date, priority, description FROM remote('127.0.0.{2,3}', test, report2)) ORDER BY id, event_date ASC;

DROP TABLE test.report1;
DROP TABLE test.report2;

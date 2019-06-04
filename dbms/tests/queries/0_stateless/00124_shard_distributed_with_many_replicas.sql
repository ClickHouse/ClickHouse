SET max_parallel_replicas = 2;

DROP TABLE IF EXISTS test.report;

CREATE TABLE test.report(id UInt32, event_date Date, priority UInt32, description String) ENGINE = MergeTree(event_date, intHash32(id), (id, event_date, intHash32(id)), 8192);

INSERT INTO test.report(id,event_date,priority,description) VALUES (1, '2015-01-01', 1, 'foo')(2, '2015-02-01', 2, 'bar')(3, '2015-03-01', 3, 'foo')(4, '2015-04-01', 4, 'bar')(5, '2015-05-01', 5, 'foo');
SELECT * FROM (SELECT id, event_date, priority, description FROM remote('127.0.0.{2|3}', test, report)) ORDER BY id ASC;

DROP TABLE test.report;


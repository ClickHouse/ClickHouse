-- Tags: shard

DROP TABLE IF EXISTS report1;
DROP TABLE IF EXISTS report2;

CREATE TABLE report1(id UInt32, event_date Date, priority UInt32, description String) ENGINE = MergeTree(event_date, intHash32(id), (id, event_date, intHash32(id)), 8192);
CREATE TABLE report2(id UInt32, event_date Date, priority UInt32, description String) ENGINE = MergeTree(event_date, intHash32(id), (id, event_date, intHash32(id)), 8192);

INSERT INTO report1(id,event_date,priority,description) VALUES (1, '2015-01-01', 1, 'foo')(2, '2015-02-01', 2, 'bar')(3, '2015-03-01', 3, 'foo')(4, '2015-04-01', 4, 'bar')(5, '2015-05-01', 5, 'foo');
INSERT INTO report2(id,event_date,priority,description) VALUES (1, '2016-01-01', 6, 'bar')(2, '2016-02-01', 7, 'foo')(3, '2016-03-01', 8, 'bar')(4, '2016-04-01', 9, 'foo')(5, '2016-05-01', 10, 'bar');

SELECT * FROM (SELECT id, event_date, priority, description FROM remote('127.0.0.{2,3}', currentDatabase(), report1) UNION ALL SELECT id, event_date, priority, description FROM remote('127.0.0.{2,3}', currentDatabase(), report2)) ORDER BY id, event_date ASC;

DROP TABLE report1;
DROP TABLE report2;

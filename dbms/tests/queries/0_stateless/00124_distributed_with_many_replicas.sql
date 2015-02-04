SET max_parallel_replicas = 2;

DROP TABLE IF EXISTS test.report;

CREATE TABLE test.report(id UInt32, event_date Date, priority UInt32, description String) ENGINE = MergeTree(event_date, intHash32(id), (id, event_date, intHash32(id)), 8192);

INSERT INTO test.report(id,event_date,priority,description) VALUES(1, '2015-01-01', 1, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(2, '2015-02-01', 2, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(3, '2015-03-01', 3, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(4, '2015-04-01', 4, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(5, '2015-05-01', 5, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(6, '2015-06-01', 6, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(7, '2015-07-01', 7, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(8, '2015-08-01', 8, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(9, '2015-09-01', 9, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(10, '2015-10-01', 10, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(11, '2015-11-01', 1, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(12, '2015-12-01', 2, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(13, '2015-01-01', 3, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(14, '2015-02-01', 4, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(15, '2015-03-01', 5, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(16, '2015-04-01', 6, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(17, '2015-05-01', 7, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(18, '2015-06-01', 8, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(19, '2015-07-01', 9, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(20, '2015-08-01', 10, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(21, '2015-09-01', 1, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(22, '2015-10-01', 2, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(23, '2015-11-01', 3, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(24, '2015-12-01', 4, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(25, '2015-01-01', 5, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(26, '2015-02-01', 6, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(27, '2015-03-01', 7, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(28, '2015-04-01', 8, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(29, '2015-05-01', 9, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(30, '2015-06-01', 10, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(31, '2015-07-01', 1, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(32, '2015-08-01', 2, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(33, '2015-09-01', 3, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(34, '2015-10-01', 4, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(35, '2015-11-01', 5, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(36, '2015-12-01', 6, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(37, '2015-01-01', 7, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(38, '2015-02-01', 8, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(39, '2015-03-01', 9, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(40, '2015-04-01', 10, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(41, '2015-05-01', 1, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(42, '2015-06-01', 2, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(43, '2015-07-01', 3, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(44, '2015-08-01', 4, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(45, '2015-09-01', 5, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(46, '2015-10-01', 6, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(47, '2015-11-01', 7, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(48, '2015-12-01', 8, 'bar');
INSERT INTO test.report(id,event_date,priority,description) VALUES(49, '2015-01-01', 9, 'foo');
INSERT INTO test.report(id,event_date,priority,description) VALUES(50, '2015-02-01', 10, 'bar');

SELECT * FROM (SELECT id, event_date, priority, description FROM remote('127.0.0.{1|2}', test, report)) ORDER BY id ASC;

DROP TABLE test.report;


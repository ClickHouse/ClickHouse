DROP TABLE IF EXISTS test.partitions;
CREATE TABLE test.partitions (EventDate Date, CounterID UInt32) ENGINE = MergeTree(EventDate, CounterID, 8192);
INSERT INTO test.partitions SELECT EventDate + UserID % 365 AS EventDate, CounterID FROM test.hits WHERE CounterID = 731962;


SELECT count() FROM test.partitions;
SELECT count() FROM test.partitions WHERE EventDate >= toDate('2015-01-01') AND EventDate < toDate('2015-02-01');
SELECT count() FROM test.partitions WHERE EventDate < toDate('2015-01-01') OR EventDate >= toDate('2015-02-01');

ALTER TABLE test.partitions DETACH PARTITION 201501;

SELECT count() FROM test.partitions;
SELECT count() FROM test.partitions WHERE EventDate >= toDate('2015-01-01') AND EventDate < toDate('2015-02-01');
SELECT count() FROM test.partitions WHERE EventDate < toDate('2015-01-01') OR EventDate >= toDate('2015-02-01');

ALTER TABLE test.partitions ATTACH PARTITION 201501;

SELECT count() FROM test.partitions;
SELECT count() FROM test.partitions WHERE EventDate >= toDate('2015-01-01') AND EventDate < toDate('2015-02-01');
SELECT count() FROM test.partitions WHERE EventDate < toDate('2015-01-01') OR EventDate >= toDate('2015-02-01');


ALTER TABLE test.partitions DETACH PARTITION 201403;

SELECT count() FROM test.partitions;

INSERT INTO test.partitions SELECT EventDate + UserID % 365 AS EventDate, CounterID FROM test.hits WHERE CounterID = 731962 AND toStartOfMonth(EventDate) = toDate('2014-03-01');

SELECT count() FROM test.partitions;

ALTER TABLE test.partitions ATTACH PARTITION 201403;

SELECT count() FROM test.partitions;


DROP TABLE test.partitions;

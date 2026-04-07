-- Tags: stateful
DROP TABLE IF EXISTS partitions;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE partitions (EventDate Date, CounterID UInt32) ENGINE = MergeTree(EventDate, CounterID, 8192);
INSERT INTO partitions SELECT EventDate + UserID % 365 AS EventDate, CounterID FROM test.hits WHERE CounterID = 1704509;


SELECT count() FROM partitions;
SELECT count() FROM partitions WHERE EventDate >= toDate('2015-01-01') AND EventDate < toDate('2015-02-01');
SELECT count() FROM partitions WHERE EventDate < toDate('2015-01-01') OR EventDate >= toDate('2015-02-01');

ALTER TABLE partitions DETACH PARTITION 201501;

SELECT count() FROM partitions;
SELECT count() FROM partitions WHERE EventDate >= toDate('2015-01-01') AND EventDate < toDate('2015-02-01');
SELECT count() FROM partitions WHERE EventDate < toDate('2015-01-01') OR EventDate >= toDate('2015-02-01');

ALTER TABLE partitions ATTACH PARTITION 201501;

SELECT count() FROM partitions;
SELECT count() FROM partitions WHERE EventDate >= toDate('2015-01-01') AND EventDate < toDate('2015-02-01');
SELECT count() FROM partitions WHERE EventDate < toDate('2015-01-01') OR EventDate >= toDate('2015-02-01');


ALTER TABLE partitions DETACH PARTITION 201403;

SELECT count() FROM partitions;

INSERT INTO partitions SELECT EventDate + UserID % 365 AS EventDate, CounterID FROM test.hits WHERE CounterID = 1704509 AND toStartOfMonth(EventDate) = toDate('2014-03-01');

SELECT count() FROM partitions;

ALTER TABLE partitions ATTACH PARTITION 201403;

SELECT count() FROM partitions;


DROP TABLE partitions;

-- Tags: stateful
SELECT count() FROM test.hits WHERE EventDate = '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate < '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate > '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate <= '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate >= '2014-03-18';
SELECT count() FROM test.hits WHERE EventDate IN ('2014-03-18', '2014-03-19');

SELECT count() FROM test.hits WHERE EventDate = toDate('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate < toDate('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate > toDate('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate <= toDate('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate >= toDate('2014-03-18');
SELECT count() FROM test.hits WHERE EventDate IN (toDate('2014-03-18'), toDate('2014-03-19'));

SELECT count() FROM test.hits WHERE EventDate = concat('2014-0', '3-18');

DROP TABLE IF EXISTS hits_indexed_by_time;
CREATE TABLE hits_indexed_by_time (EventDate Date, EventTime DateTime('Asia/Dubai')) ENGINE = MergeTree ORDER BY (EventDate, EventTime) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO hits_indexed_by_time SELECT EventDate, EventTime FROM test.hits SETTINGS max_block_size = 65000;

SELECT count() FROM hits_indexed_by_time WHERE EventTime = '2014-03-18 01:02:03';
SELECT count() FROM hits_indexed_by_time WHERE EventTime < '2014-03-18 01:02:03';
SELECT count() FROM hits_indexed_by_time WHERE EventTime > '2014-03-18 01:02:03';
SELECT count() FROM hits_indexed_by_time WHERE EventTime <= '2014-03-18 01:02:03';
SELECT count() FROM hits_indexed_by_time WHERE EventTime >= '2014-03-18 01:02:03';
SELECT count() FROM hits_indexed_by_time WHERE EventTime IN ('2014-03-18 01:02:03', '2014-03-19 04:05:06');

SELECT count() FROM hits_indexed_by_time WHERE EventTime = toDateTime('2014-03-18 01:02:03', 'Asia/Dubai');
SELECT count() FROM hits_indexed_by_time WHERE EventTime < toDateTime('2014-03-18 01:02:03', 'Asia/Dubai');
SELECT count() FROM hits_indexed_by_time WHERE EventTime > toDateTime('2014-03-18 01:02:03', 'Asia/Dubai');
SELECT count() FROM hits_indexed_by_time WHERE EventTime <= toDateTime('2014-03-18 01:02:03', 'Asia/Dubai');
SELECT count() FROM hits_indexed_by_time WHERE EventTime >= toDateTime('2014-03-18 01:02:03', 'Asia/Dubai');
SELECT count() FROM hits_indexed_by_time WHERE EventTime IN (toDateTime('2014-03-18 01:02:03', 'Asia/Dubai'), toDateTime('2014-03-19 04:05:06', 'Asia/Dubai'));

SELECT count() FROM hits_indexed_by_time WHERE EventTime = concat('2014-03-18 ', '01:02:03');

DROP TABLE hits_indexed_by_time;

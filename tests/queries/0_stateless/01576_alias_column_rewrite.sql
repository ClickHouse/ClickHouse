DROP TABLE IF EXISTS table_with_alias_column;
CREATE TABLE table_with_alias_column
(
 `timestamp` DateTime,
 `value` UInt64,
 `day` Date ALIAS toDate(timestamp),
 `day1` Date ALIAS day + 1,
 `day2` Date ALIAS day1 + 1,
 `time` DateTime ALIAS timestamp
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY timestamp;


INSERT INTO table_with_alias_column(timestamp, value) SELECT toDateTime('2020-01-01 12:00:00'), 1 FROM numbers(1000);

INSERT INTO table_with_alias_column(timestamp, value) SELECT toDateTime('2020-01-02 12:00:00'), 1 FROM numbers(1000);

INSERT INTO table_with_alias_column(timestamp, value) SELECT toDateTime('2020-01-03 12:00:00'), 1 FROM numbers(1000);


SELECT 'test-partition-prune';

SELECT COUNT() = 1000 FROM table_with_alias_column WHERE day = '2020-01-01' SETTINGS max_rows_to_read = 1000;
SELECT t = '2020-01-03' FROM (SELECT day as t FROM table_with_alias_column WHERE t = '2020-01-03' GROUP BY t SETTINGS max_rows_to_read = 1000);
SELECT COUNT() = 1000 FROM table_with_alias_column WHERE day = '2020-01-01' UNION ALL select 1 from numbers(1) SETTINGS max_rows_to_read = 1001;
SELECT  COUNT() = 0 FROM (SELECT  toDate('2019-01-01') as  day, day as t   FROM table_with_alias_column PREWHERE t = '2020-01-03'  WHERE t  = '2020-01-03' GROUP BY t );

SELECT 'test-join';

SELECT day = '2020-01-03'
FROM
(
 SELECT toDate('2020-01-03') AS day
 FROM numbers(1)
) AS a
INNER JOIN
(
 SELECT day
 FROM table_with_alias_column
 WHERE day = '2020-01-03'
 GROUP BY day SETTINGS max_rows_to_read = 1000
) AS b ON a.day = b.day;

SELECT day = '2020-01-01'
FROM
(
 SELECT day
 FROM table_with_alias_column
 WHERE day = '2020-01-01'
 GROUP BY day SETTINGS max_rows_to_read = 1001
) AS a
INNER JOIN
(
 SELECT toDate('2020-01-01') AS day
 FROM numbers(1)
) AS b ON a.day = b.day;


SELECT 'alias2alias';
SELECT COUNT() = 1000 FROM table_with_alias_column WHERE day1 = '2020-01-02' SETTINGS max_rows_to_read = 1000;
SELECT t = '2020-01-03' FROM (SELECT day1 as t FROM table_with_alias_column WHERE t = '2020-01-03' GROUP BY t SETTINGS max_rows_to_read = 1000);
SELECT t = '2020-01-03' FROM (SELECT day2 as t FROM table_with_alias_column WHERE t = '2020-01-03' GROUP BY t SETTINGS max_rows_to_read = 1000);
SELECT COUNT() = 1000 FROM table_with_alias_column WHERE day1 = '2020-01-03' UNION ALL select 1 from numbers(1) SETTINGS max_rows_to_read = 1001;
SELECT  COUNT() = 0 FROM (SELECT  toDate('2019-01-01') as  day1, day1 as t   FROM table_with_alias_column PREWHERE t = '2020-01-03'  WHERE t  = '2020-01-03' GROUP BY t );
SELECT day1 = '2020-01-04' FROM table_with_alias_column PREWHERE day1 = '2020-01-04'  WHERE day1 = '2020-01-04' GROUP BY day1 SETTINGS max_rows_to_read = 1000;

DROP TABLE table_with_alias_column;


SELECT 'second_index';

DROP TABLE IF EXISTS test_index;
CREATE TABLE test_index
(
    `key_string` String,
    `key_uint32` ALIAS toUInt32(key_string),
    INDEX idx toUInt32(key_string) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY tuple()
PRIMARY KEY tuple()
ORDER BY key_string SETTINGS index_granularity = 1;

INSERT INTO test_index SELECT * FROM numbers(10);
SELECT COUNT() == 1 FROM test_index WHERE key_uint32 = 1 SETTINGS max_rows_to_read = 1;
SELECT COUNT() == 1 FROM test_index WHERE toUInt32(key_string) = 1 SETTINGS max_rows_to_read = 1;
DROP TABLE IF EXISTS test_index;

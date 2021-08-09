DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
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
ORDER BY timestamp SETTINGS index_granularity = 1;


INSERT INTO test_table(timestamp, value) SELECT toDateTime('2020-01-01 12:00:00'), 1 FROM numbers(10);
INSERT INTO test_table(timestamp, value) SELECT toDateTime('2020-01-02 12:00:00'), 1 FROM numbers(10);
INSERT INTO test_table(timestamp, value) SELECT toDateTime('2020-01-03 12:00:00'), 1 FROM numbers(10);

set optimize_respect_aliases = 1;
SELECT 'test-partition-prune';

SELECT COUNT() = 10 FROM test_table WHERE day = '2020-01-01' SETTINGS max_rows_to_read = 10;
SELECT t = '2020-01-03' FROM (SELECT day AS t FROM test_table WHERE t = '2020-01-03' GROUP BY t SETTINGS max_rows_to_read = 10);
SELECT COUNT() = 10 FROM test_table WHERE day = '2020-01-01' UNION ALL SELECT 1 FROM numbers(1) SETTINGS max_rows_to_read = 11;
SELECT  COUNT() = 0 FROM (SELECT  toDate('2019-01-01') AS  day, day AS t   FROM test_table PREWHERE t = '2020-01-03'  WHERE t  = '2020-01-03' GROUP BY t );



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
 FROM test_table
 WHERE day = '2020-01-03'
 GROUP BY day
) AS b ON a.day = b.day SETTINGS max_rows_to_read = 11;

SELECT day = '2020-01-01'
FROM
(
 SELECT day
 FROM test_table
 WHERE day = '2020-01-01'
 GROUP BY day
) AS a
INNER JOIN
(
 SELECT toDate('2020-01-01') AS day
 FROM numbers(1)
) AS b ON a.day = b.day SETTINGS max_rows_to_read = 11;


SELECT 'alias2alias';
SELECT COUNT() = 10 FROM test_table WHERE day1 = '2020-01-02' SETTINGS max_rows_to_read = 10;
SELECT t = '2020-01-03' FROM (SELECT day1 AS t FROM test_table WHERE t = '2020-01-03' GROUP BY t SETTINGS max_rows_to_read = 10);
SELECT t = '2020-01-03' FROM (SELECT day2 AS t FROM test_table WHERE t = '2020-01-03' GROUP BY t SETTINGS max_rows_to_read = 10);
SELECT COUNT() = 10 FROM test_table WHERE day1 = '2020-01-03' UNION ALL SELECT 1 FROM numbers(1) SETTINGS max_rows_to_read = 11;
SELECT  COUNT() = 0 FROM (SELECT  toDate('2019-01-01') AS  day1, day1 AS t   FROM test_table PREWHERE t = '2020-01-03'  WHERE t  = '2020-01-03' GROUP BY t );
SELECT day1 = '2020-01-04' FROM test_table PREWHERE day1 = '2020-01-04'  WHERE day1 = '2020-01-04' GROUP BY day1 SETTINGS max_rows_to_read = 10;


ALTER TABLE test_table add column array Array(UInt8) default [1, 2, 3];
ALTER TABLE test_table add column struct.key Array(UInt8) default [2, 4, 6], add column struct.value Array(UInt8) alias array;


SELECT 'array-join';
set max_rows_to_read = 10;
SELECT count() == 10 FROM test_table WHERE day = '2020-01-01';
SELECT sum(struct.key) == 30, sum(struct.value) == 30 FROM (SELECT struct.key, struct.value FROM test_table array join struct WHERE day = '2020-01-01');


SELECT 'lambda';
-- lambda parameters in filter should not be rewrite
SELECT count() == 10 FROM test_table WHERE  arrayMap((day) -> day + 1, [1,2,3]) [1] = 2 AND day = '2020-01-03';

set max_rows_to_read = 0;

SELECT 'optimize_read_in_order';
EXPLAIN SELECT day AS s FROM test_table ORDER BY s LIMIT 1 SETTINGS optimize_read_in_order = 0;
EXPLAIN SELECT day AS s FROM test_table ORDER BY s LIMIT 1 SETTINGS optimize_read_in_order = 1;
EXPLAIN SELECT toDate(timestamp) AS s FROM test_table ORDER BY toDate(timestamp) LIMIT 1 SETTINGS optimize_read_in_order = 1;


SELECT 'optimize_aggregation_in_order';
EXPLAIN SELECT day, count() AS s FROM test_table GROUP BY day SETTINGS optimize_aggregation_in_order = 0;
EXPLAIN SELECT day, count() AS s FROM test_table GROUP BY day SETTINGS optimize_aggregation_in_order = 1;
EXPLAIN SELECT toDate(timestamp), count() AS s FROM test_table GROUP BY toDate(timestamp) SETTINGS optimize_aggregation_in_order = 1;

DROP TABLE test_table;


SELECT 'second-index';
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
set max_rows_to_read = 1;
SELECT COUNT() == 1 FROM test_index WHERE key_uint32 = 1;
SELECT COUNT() == 1 FROM test_index WHERE toUInt32(key_string) = 1;
DROP TABLE IF EXISTS test_index;


-- check alias column can be used to match projections
drop table if exists p;
create table pd (dt DateTime, i int, dt_m DateTime alias toStartOfMinute(dt)) engine Distributed(test_shard_localhost, currentDatabase(), 'pl');
create table pl (dt DateTime, i int, projection p (select sum(i) group by toStartOfMinute(dt))) engine MergeTree order by dt;

insert into pl values ('2020-10-24', 1);

select sum(i) from pd group by dt_m settings allow_experimental_projection_optimization = 1, force_optimize_projection = 1;

drop table pd;
drop table pl;

drop table if exists t;

create temporary table t (x UInt64, y alias x);
insert into t values (1);
select sum(x), sum(y) from t;

drop table t;

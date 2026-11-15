SET enable_lazy_columns_replication=0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id UInt32, key Int32, payload String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 SELECT rand(), 1, 'a' || if(number = 99, repeat(toString(number), 1000), toString(number)) FROM numbers(100);

CREATE TABLE t2 (id UInt32, key Int32, payload String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 SELECT rand(), 1, 'b' || toString(number) FROM numbers(100_000);

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 1;

SELECT
    -- blocks with much of data are small in rows:
    if(max(size) < 5_000_000 AND argMax(rows, size) < 10_000, 'Ok', format('Error: max_size={} rows={}', max(size), argMax(rows, size))),
    -- but still there are large blocks with small strings
    if(max(rows) >= 50_000, 'Ok', format('Error: {}', toString(max(rows))))
FROM ( SELECT blockNumber() as bn, sum(byteSize(*)) as size, count() as rows FROM t1 INNER JOIN t2 ON t1.key = t2.key GROUP BY bn )
SETTINGS joined_block_split_single_row = 1
    , max_joined_block_size_bytes = '4M'
    , max_joined_block_size_rows = 65_000;


-- add 3Mb match
INSERT INTO t1 SELECT rand(), 2, repeat('aaa', 1_000_000);
INSERT INTO t2 SELECT rand(), 2, repeat('bbb', 1_000_000) from numbers(10);

SELECT
    -- limit is 4M but minimum block size is 6Mb, so it will be single row block
    if(argMax(rows, size) = 1 AND max(size) < 10_000_000, 'Ok', format('Error: max_size={} rows={}', max(size), argMax(rows, size)))
FROM ( SELECT blockNumber() as bn, sum(byteSize(*)) as size, count() as rows FROM t1 INNER JOIN t2 ON t1.key = t2.key GROUP BY bn )
SETTINGS joined_block_split_single_row = 1, max_joined_block_size_bytes = '4M';

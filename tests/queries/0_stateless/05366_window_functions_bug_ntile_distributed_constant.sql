-- ntile rejects its constant bucket count when the query reads from more than one shard.
-- This test pins the current behaviour so that a fix has to update the reference.
-- https://github.com/ClickHouse/ClickHouse/issues/112401

CREATE TABLE t (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT number FROM numbers(4);

SELECT '-- the query works on the local table';
SELECT k, ntile(2) OVER (ORDER BY k) FROM t SETTINGS enable_parallel_replicas = 0;

SELECT '-- the same query over two shards is rejected';
SELECT k, ntile(2) OVER (ORDER BY k) FROM remote('127.0.0.{1,2}', currentDatabase(), t); -- { serverError BAD_ARGUMENTS }

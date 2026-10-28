-- The bucket count of ntile stays usable when the query reads from more than one shard.
-- https://github.com/ClickHouse/ClickHouse/issues/112401

CREATE TABLE t (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT number FROM numbers(4);

SELECT '-- the query works on the local table';
SELECT k, ntile(2) OVER (ORDER BY k) FROM t SETTINGS enable_parallel_replicas = 0;

SELECT '-- the same query over two shards';
SELECT k, ntile(2) OVER (ORDER BY k) FROM remote('127.0.0.{1,2}', currentDatabase(), t) ORDER BY k SETTINGS enable_parallel_replicas = 0;

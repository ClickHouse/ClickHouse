SET enable_analyzer=0;

SELECT 'Old Analyzer';

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01 OFFSET 0.9;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 3 OFFSET 0.9;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01 OFFSET 297;

SET enable_analyzer=1;

SELECT 'Analyzer';

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01 OFFSET 0.9;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 3 OFFSET 0.9;

SELECT number from remote('127.0.0.{1,2,3}', numbers_mt(100)) ORDER BY number LIMIT 0.01 OFFSET 297;

-- Distributed Table

SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS test__fuzz_2_local;
DROP TABLE IF EXISTS test__fuzz_2_dist;

CREATE TABLE test__fuzz_2_local
(
    k UInt64
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1;

INSERT INTO test__fuzz_2_local VALUES (1),(2),(3);

CREATE TABLE test__fuzz_2_dist AS test__fuzz_2_local
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), test__fuzz_2_local, rand());

SELECT 'Old Analyzer - Distributed Table';

SET enable_analyzer = 0;
SET extremes = 1;

SELECT 0 AS `0`, 18 AS `18`, __table1.k AS k
FROM test__fuzz_2_dist AS __table1
PREWHERE _CAST(11, 'Nullable(UInt8)')
ORDER BY __table1.k DESC NULLS LAST
LIMIT 0.9999, _CAST(100, 'UInt64');

SET extremes = 0;
SELECT 'Analyzer - Distributed Table';

SET enable_analyzer = 1;
SET extremes = 1;

SELECT 0 AS `0`, 18 AS `18`, __table1.k AS k
FROM test__fuzz_2_dist AS __table1
PREWHERE _CAST(11, 'Nullable(UInt8)')
ORDER BY __table1.k DESC NULLS LAST
LIMIT 0.9999, _CAST(100, 'UInt64');

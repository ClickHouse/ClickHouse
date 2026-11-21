-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: settings randomization changes whether the cache is used at all.

-- An on-fly mutation keeps the part name and `info.mutation` until it is materialized,
-- so a partial aggregate cache entry warmed before it must not be reused while it is pending.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_pac_on_fly;

CREATE TABLE test_pac_on_fly (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES test_pac_on_fly;

INSERT INTO test_pac_on_fly SELECT number, 1 FROM numbers(100);
INSERT INTO test_pac_on_fly SELECT number + 100, 1 FROM numbers(100);

SET use_partial_aggregate_cache = 1;
SET apply_mutations_on_fly = 1;

SELECT '--- warm';
SELECT k % 2 AS g, sum(v), count() FROM test_pac_on_fly GROUP BY g ORDER BY g;
SELECT k % 2 AS g, sum(v), count() FROM test_pac_on_fly GROUP BY g ORDER BY g;

ALTER TABLE test_pac_on_fly UPDATE v = 10 WHERE k < 50 SETTINGS mutations_sync = 0;

SELECT '--- after UPDATE applied on fly';
SELECT k % 2 AS g, sum(v), count() FROM test_pac_on_fly GROUP BY g ORDER BY g;
SELECT k % 2 AS g, sum(v), count() FROM test_pac_on_fly GROUP BY g ORDER BY g;

ALTER TABLE test_pac_on_fly DELETE WHERE k >= 150 SETTINGS mutations_sync = 0;

SELECT '--- after DELETE applied on fly';
SELECT k % 2 AS g, sum(v), count() FROM test_pac_on_fly GROUP BY g ORDER BY g;

DROP TABLE test_pac_on_fly;

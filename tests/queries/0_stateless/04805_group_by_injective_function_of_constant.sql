-- Tags: shard

DROP TABLE IF EXISTS local_t;
DROP TABLE IF EXISTS dist_t;
DROP TABLE IF EXISTS empty_t;

CREATE TABLE local_t (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO local_t SELECT number FROM numbers(10);
CREATE TABLE empty_t (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dist_t (k UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local_t);

-- Pin every setting the assertions depend on: the runner randomizes them, and with the
-- optimization disabled the unfixed code is already correct, so a randomized run would pass
-- against it. The two-level thresholds decide whether the memory-efficient path is reached.
SET enable_analyzer = 1;
SET optimize_injective_functions_in_group_by = 1;
SET group_by_use_nulls = 0;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;

SELECT '-- a GROUP BY key that is an injective function of constants must still aggregate';
SELECT count() FROM (SELECT 1 FROM local_t GROUP BY materialize(NULL));
SELECT count() FROM (SELECT 1 FROM local_t GROUP BY toString(materialize(NULL)));
SELECT count() FROM (SELECT 1 FROM local_t GROUP BY materialize(1));
SELECT count() FROM (SELECT 1 FROM local_t GROUP BY materialize('a'), materialize('b'));
SELECT count() FROM (SELECT materialize(toNullable(NULL)) AS x FROM local_t GROUP BY ALL);

SELECT '-- with an aggregate function';
SELECT count() AS c FROM local_t GROUP BY materialize(NULL);
SELECT count() FROM (SELECT count() FROM empty_t GROUP BY materialize(NULL));

SELECT '-- a surviving key still drops the constant-derived one';
SELECT count() FROM (SELECT 1 FROM local_t GROUP BY materialize(NULL), k);

SELECT '-- the injective unwrap still applies where it is valid';
SELECT countIf(explain ILIKE '%function_name: toString%') FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM local_t GROUP BY toString(k)) SETTINGS enable_analyzer = 1, optimize_injective_functions_in_group_by = 1;
SELECT countIf(explain ILIKE '%function_name: materialize%') FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM local_t GROUP BY materialize(NULL), k) SETTINGS enable_analyzer = 1, optimize_injective_functions_in_group_by = 1;
SELECT countIf(explain ILIKE '%function_name: materialize%') > 0 FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT 1 FROM local_t GROUP BY materialize(NULL)) SETTINGS enable_analyzer = 1, optimize_injective_functions_in_group_by = 1;

SELECT '-- LIMIT BY shares the unwrap helper and is unaffected';
SELECT count() FROM (SELECT k FROM local_t ORDER BY k LIMIT 1 BY materialize(NULL));
SELECT count() FROM (SELECT k FROM local_t ORDER BY k LIMIT 1 BY toString(k));

SELECT '-- GROUP BY modifiers keep their own behaviour';
SELECT count() FROM (SELECT 1 FROM local_t GROUP BY materialize(NULL) WITH CUBE);
SELECT count() FROM (SELECT 1 FROM local_t GROUP BY materialize(NULL) WITH ROLLUP);
SELECT count() FROM (SELECT 1 FROM local_t GROUP BY materialize(NULL) WITH TOTALS);
SELECT count() FROM (SELECT 1 FROM local_t GROUP BY materialize(NULL)) SETTINGS group_by_use_nulls = 1;

SELECT '-- a Merge table mixing a local and a remote child must not lose the aggregation';
SELECT multiIf(0, NULL, materialize(toNullable(NULL))) FROM merge(currentDatabase(), '^(local_t|dist_t)$') GROUP BY ALL SETTINGS distributed_aggregation_memory_efficient = 1;
SELECT multiIf(0, NULL, materialize(toNullable(NULL))) FROM merge(currentDatabase(), '^(local_t|dist_t)$') GROUP BY ALL SETTINGS distributed_aggregation_memory_efficient = 0;
SELECT count() FROM (SELECT 1 FROM merge(currentDatabase(), '^(local_t|dist_t)$') GROUP BY materialize(toNullable(NULL)));

SELECT '-- and the same shapes without the analyzer';
SELECT count() FROM (SELECT 1 FROM local_t GROUP BY materialize(NULL)) SETTINGS enable_analyzer = 0;
SELECT count() FROM (SELECT 1 FROM merge(currentDatabase(), '^(local_t|dist_t)$') GROUP BY materialize(toNullable(NULL))) SETTINGS enable_analyzer = 0;

DROP TABLE dist_t;
DROP TABLE local_t;
DROP TABLE empty_t;

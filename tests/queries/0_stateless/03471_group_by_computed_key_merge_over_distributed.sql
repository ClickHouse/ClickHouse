-- Tags: shard

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111272:
-- a GROUP BY with a computed key over column X plus an aggregate over the same X,
-- against a Merge table wrapping a Distributed table, must return the same result
-- as the query on the Distributed table directly (it used to throw
-- NUMBER_OF_COLUMNS_DOESNT_MATCH under the analyzer).

DROP TABLE IF EXISTS t_local_111272;
DROP TABLE IF EXISTS t_dist_111272;
DROP TABLE IF EXISTS t_merge_111272;

CREATE TABLE t_local_111272 (g UInt16, v Int64) ENGINE = MergeTree ORDER BY g;
INSERT INTO t_local_111272 SELECT number % 8, number FROM numbers(100);

CREATE TABLE t_dist_111272 AS t_local_111272
    ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_local_111272);

CREATE TABLE t_merge_111272 AS t_dist_111272
    ENGINE = Merge(currentDatabase(), '^t_dist_111272$');

-- Both optimizer passes must stay enabled: the mismatch only appeared when
-- OptimizeGroupByInjectiveFunctionsPass rewrites GROUP BY toString(g) into GROUP BY g
-- and AggregateFunctionOfGroupByKeysPass then folds min(g). The test runner may
-- randomly disable either, which would let this test pass against unfixed code.
SET enable_analyzer = 1;
SET optimize_injective_functions_in_group_by = 1;
SET optimize_aggregators_of_group_by_keys = 1;
SET group_by_use_nulls = 0;

-- Minimal trigger: computed key over g + aggregate over the same g.
SELECT toString(g), min(g) FROM t_merge_111272 GROUP BY toString(g) ORDER BY 1;

-- Original issue query: nested computed key + a plain column + aggregate over g.
SELECT toString(toString(g)), v, min(g) FROM t_merge_111272
WHERE g < 447 GROUP BY toString(toString(g)), v ORDER BY 1, 2 LIMIT 5;

-- Merge-over-Distributed result must match the Distributed result exactly (both directions).
SELECT count() FROM (
    (SELECT toString(g), min(g), max(g), count() FROM t_merge_111272 GROUP BY toString(g))
    EXCEPT
    (SELECT toString(g), min(g), max(g), count() FROM t_dist_111272 GROUP BY toString(g))
);
SELECT count() FROM (
    (SELECT toString(g), min(g), max(g), count() FROM t_dist_111272 GROUP BY toString(g))
    EXCEPT
    (SELECT toString(g), min(g), max(g), count() FROM t_merge_111272 GROUP BY toString(g))
);

DROP TABLE t_merge_111272;
DROP TABLE t_dist_111272;
DROP TABLE t_local_111272;

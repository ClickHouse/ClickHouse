-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: `UNION DISTINCT` + `GROUPING SETS` must not trigger hash mismatch exceptions.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_union_gs;

CREATE TABLE test_partial_agg_union_gs
(
    date Date,
    category String,
    value Int64
)
ENGINE = MergeTree()
ORDER BY (date, category);

SYSTEM STOP MERGES test_partial_agg_union_gs;

SET allow_experimental_analyzer = 1;
SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_union_gs VALUES ('2024-01-01', 'A', 1), ('2024-01-01', 'A', 2), ('2024-01-01', 'B', 10), ('2024-01-01', 'B', 20);

SELECT '--- Warm non-grouping-sets query';

SELECT count() AS c, category, sum(value) AS s
FROM test_partial_agg_union_gs
GROUP BY category
ORDER BY category
SETTINGS
    allow_experimental_analyzer = 1,
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_union_gs_warm';

SELECT '--- UNION DISTINCT with GROUPING SETS: must succeed and return deterministic result';

SELECT *
FROM
(
    SELECT DISTINCT count() AS c, category, sum(value) AS s
    FROM test_partial_agg_union_gs
    GROUP BY GROUPING SETS ((category), ())

    UNION DISTINCT

    SELECT DISTINCT count() AS c, category, sum(value) AS s
    FROM test_partial_agg_union_gs
    GROUP BY category
)
ORDER BY c, category, s
SETTINGS
    allow_experimental_analyzer = 1,
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_union_gs_union_q1';

DROP TABLE test_partial_agg_union_gs;

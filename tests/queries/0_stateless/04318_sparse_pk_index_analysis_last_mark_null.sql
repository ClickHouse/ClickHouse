-- Tags: no-replicated-database, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- EXPLAIN output may differ

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_sparse_pk_last_mark_null;

CREATE TABLE t_sparse_pk_last_mark_null
(
    a Nullable(Int64),
    b Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 2, index_granularity_bytes = 0, allow_nullable_key = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_sparse_pk_last_mark_null VALUES (1, 1), (2, 2), (NULL, NULL), (NULL, NULL), (NULL, NULL), (NULL, NULL);

OPTIMIZE TABLE t_sparse_pk_last_mark_null FINAL;

-- { echoOn }

EXPLAIN projections = 1
SELECT count() FROM t_sparse_pk_last_mark_null
WHERE a IS NULL AND b IS NULL
SETTINGS use_lightweight_primary_key_index_analysis = 0, optimize_use_implicit_projections = 1;

SELECT count() FROM t_sparse_pk_last_mark_null
WHERE a IS NULL AND b IS NULL
SETTINGS use_lightweight_primary_key_index_analysis = 0, optimize_use_implicit_projections = 1;

EXPLAIN projections = 1
SELECT count() FROM t_sparse_pk_last_mark_null
WHERE a IS NULL AND b IS NULL
SETTINGS use_lightweight_primary_key_index_analysis = 1, optimize_use_implicit_projections = 1;

SELECT count() FROM t_sparse_pk_last_mark_null
WHERE a IS NULL AND b IS NULL
SETTINGS use_lightweight_primary_key_index_analysis = 1, optimize_use_implicit_projections = 1;

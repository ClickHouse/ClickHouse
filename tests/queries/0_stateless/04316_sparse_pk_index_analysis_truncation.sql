-- Tags: no-replicated-database, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- EXPLAIN output may differ

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_sparse_pk_truncation;

CREATE TABLE t_sparse_pk_truncation
(
    c0 Int32,
    c1 Int32
)
ENGINE = MergeTree
ORDER BY (c0, c1)
SETTINGS index_granularity = 1;

INSERT INTO t_sparse_pk_truncation VALUES (1, 1), (2, 2), (3, 3), (4, 4);

-- { echoOn }

EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_truncation
WHERE c0 <= 1 AND c0 >= 3
SETTINGS use_lightweight_primary_key_index_analysis = 0;

SELECT count() FROM t_sparse_pk_truncation
WHERE c0 <= 1 AND c0 >= 3
SETTINGS use_lightweight_primary_key_index_analysis = 0;

EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_truncation
WHERE c0 <= 1 AND c0 >= 3
SETTINGS use_lightweight_primary_key_index_analysis = 1;

SELECT count() FROM t_sparse_pk_truncation
WHERE c0 <= 1 AND c0 >= 3
SETTINGS use_lightweight_primary_key_index_analysis = 1;

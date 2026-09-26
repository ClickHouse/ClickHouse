-- Tags: no-replicated-database, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- EXPLAIN output may differ

DROP TABLE IF EXISTS t_sparse_pk_const_false;

CREATE TABLE t_sparse_pk_const_false
(
    a UInt32,
    b UInt32
)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 1;

INSERT INTO t_sparse_pk_const_false SELECT number, number FROM numbers(100);

-- { echoOn }

EXPLAIN ESTIMATE
SELECT count() FROM t_sparse_pk_const_false
WHERE 0
SETTINGS use_lightweight_primary_key_index_analysis = 0;

SELECT count() FROM t_sparse_pk_const_false
WHERE 0
SETTINGS use_lightweight_primary_key_index_analysis = 0;

EXPLAIN ESTIMATE
SELECT count() FROM t_sparse_pk_const_false
WHERE 0
SETTINGS use_lightweight_primary_key_index_analysis = 1;

SELECT count() FROM t_sparse_pk_const_false
WHERE 0
SETTINGS use_lightweight_primary_key_index_analysis = 1;

-- Tags: no-replicated-database, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- A filter that references no primary key column but folds to a constant `false`.
-- `ignore(c)` (a non-key column) is FUNCTION_UNKNOWN, so the condition uses no key
-- columns; `AND 0` makes the whole key-condition RPN always false. Unlike a plain
-- `WHERE 0`, this is not const-folded away before index analysis, so it reaches
-- `KeyCondition`. The sparse representation (no key columns are materialized) must
-- still prune every granule.

DROP TABLE IF EXISTS t_sparse_pk_unknown_false;

CREATE TABLE t_sparse_pk_unknown_false
(
    a UInt32,
    b UInt32,
    c UInt32
)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 1;

INSERT INTO t_sparse_pk_unknown_false SELECT number, number, number FROM numbers(100);

-- { echo }

EXPLAIN ESTIMATE
SELECT count() FROM t_sparse_pk_unknown_false
WHERE ignore(c) AND 0
SETTINGS use_lightweight_primary_key_index_analysis = 0;

SELECT count() FROM t_sparse_pk_unknown_false
WHERE ignore(c) AND 0
SETTINGS use_lightweight_primary_key_index_analysis = 0;

EXPLAIN ESTIMATE
SELECT count() FROM t_sparse_pk_unknown_false
WHERE ignore(c) AND 0
SETTINGS use_lightweight_primary_key_index_analysis = 1;

SELECT count() FROM t_sparse_pk_unknown_false
WHERE ignore(c) AND 0
SETTINGS use_lightweight_primary_key_index_analysis = 1;

-- Tags: no-replicated-database, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- EXPLAIN output may differ

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_sparse_pk_sfc;

CREATE TABLE t_sparse_pk_sfc
(
    x UInt32,
    y UInt32
)
ENGINE = MergeTree
ORDER BY mortonEncode(x, y)
SETTINGS index_granularity = 4;

INSERT INTO t_sparse_pk_sfc SELECT number % 16, intDiv(number, 16) % 16 FROM numbers(256);

-- { echoOn }

EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_sfc
WHERE x >= 4 AND x <= 8 AND y >= 4 AND y <= 8
SETTINGS use_lightweight_primary_key_index_analysis = 0;

SELECT count() FROM t_sparse_pk_sfc
WHERE x >= 4 AND x <= 8 AND y >= 4 AND y <= 8
SETTINGS use_lightweight_primary_key_index_analysis = 0;

EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_sfc
WHERE x >= 4 AND x <= 8 AND y >= 4 AND y <= 8
SETTINGS use_lightweight_primary_key_index_analysis = 1;

SELECT count() FROM t_sparse_pk_sfc
WHERE x >= 4 AND x <= 8 AND y >= 4 AND y <= 8
SETTINGS use_lightweight_primary_key_index_analysis = 1;

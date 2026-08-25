-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.
-- add_minmax_index_for_numeric_columns=0: Different plan

-- { echo }

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
  id UInt64
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns=0;

INSERT INTO tab
SELECT number
FROM numbers(100);

SET use_primary_key = 0;

SELECT count() FROM tab WHERE id = 5;

EXPLAIN indexes = 1
SELECT count() FROM tab WHERE id = 5;

SET use_primary_key = 1;

SELECT count() FROM tab WHERE id = 5;

EXPLAIN indexes = 1
SELECT count() FROM tab WHERE id = 5;

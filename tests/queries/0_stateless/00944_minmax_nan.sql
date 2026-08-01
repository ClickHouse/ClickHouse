-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

SET parallel_replicas_local_plan = 1;

-- Test for issue #75523

-- { echo }

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
  id UInt64,
  col Float,
  INDEX col_idx col TYPE minmax
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES
    (1, 1.0),
    (2, inf),
    (3, 2.0),
    (4, -inf),
    (5, 3.0),
    (6, nan),
    (7, -nan);

-- MinMax is not used anymore because NaN handling optimization happens in Primary Key analysis phase
SELECT count() FROM tab WHERE col <> nan SETTINGS force_data_skipping_indices='col_idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM tab WHERE col = nan;
EXPLAIN indexes=1
SELECT count() FROM tab WHERE col = nan;

SELECT count() FROM tab WHERE col = -nan;
EXPLAIN indexes=1
SELECT count() FROM tab WHERE col = -nan;

SELECT count() FROM tab WHERE col <> nan;
EXPLAIN indexes=1
SELECT count() FROM tab WHERE col <> nan;

SELECT count() FROM tab WHERE col < nan;
EXPLAIN indexes=1
SELECT count() FROM tab WHERE col < nan;

SELECT count() FROM tab WHERE col <> -nan;
EXPLAIN indexes=1
SELECT count() FROM tab WHERE col <> -nan;

SELECT count() FROM tab WHERE isNaN(col);
SELECT count() FROM tab WHERE NOT isNaN(col);

SELECT 'Nullable Float NaN comparison';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
  col Nullable(Float)
)
ENGINE = MergeTree()
ORDER BY col
SETTINGS allow_nullable_key = 1, index_granularity = 1;

INSERT INTO tab VALUES
    (NULL),
    (inf),
    (2.0),
    (-inf),
    (3.0),
    (nan),
    (-nan),
    (NULL);

SELECT count() FROM tab WHERE col = nan;
EXPLAIN indexes = 1
SELECT count() FROM tab WHERE col = nan;

SELECT count() FROM tab WHERE col < nan;
EXPLAIN indexes = 1
SELECT count() FROM tab WHERE col < nan;

SELECT count() FROM tab WHERE col <> nan;
EXPLAIN indexes = 1
SELECT count() FROM tab WHERE col <> nan;

SELECT count() FROM tab WHERE col < nan;
SELECT count() FROM tab WHERE col = -nan;
SELECT count() FROM tab WHERE col <> -nan;
SELECT count() FROM tab WHERE isNaN(col);
SELECT count() FROM tab WHERE NOT isNaN(col);

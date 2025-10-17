SET parallel_replicas_local_plan = 1;

SET use_query_condition_cache = 0;

-- Force using skip indexes in planning to proper test `force_data_skipping_indices` setting.
SET use_skip_indexes_on_data_read = 0;

-- Test for issue #75523

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
  id UInt64,
  col Float,
  INDEX col_idx col TYPE minmax
)
ENGINE = MergeTree()
ORDER BY id; -- This is important. We want to have additional primary index that does not use the column `col`.

INSERT INTO tab VALUES
    (1, 1.0),
    (2, inf),
    (3, 2.0),
    (4, -inf),
    (5, 3.0),
    (6, nan),
    (7, -nan);

SELECT 'NaN comparison';
SELECT count() FROM tab WHERE col = nan;
SELECT count() FROM tab WHERE col <> nan;
SELECT count() FROM tab WHERE col = -nan;
SELECT count() FROM tab WHERE col <> -nan;
SELECT count() FROM tab WHERE isNaN(col);
SELECT count() FROM tab WHERE NOT isNaN(col);

SELECT 'MinMax index should skip all granules for column = NaN comparison';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE col = nan
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3; -- Skip the primary index parts and granules.

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE col = -nan
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3; -- Skip the primary index parts and granules.

SELECT 'MinMax index should use all granules for column <> NaN comparison';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE col <> nan
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3; -- Skip the primary index parts and granules.

SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE col <> -nan
)
WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3; -- Skip the primary index parts and granules.

DROP TABLE tab;

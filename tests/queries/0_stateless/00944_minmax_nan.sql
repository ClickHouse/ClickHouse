DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
  id UInt64,
  col Float,
  INDEX col_idx col TYPE minmax
)
ENGINE = MergeTree()
ORDER BY id; -- This is important. We want to have additional primary index that does not use the column `col`.

INSERT INTO tab VALUES (1, 1.0), (2, inf), (3, 2.0), (4, -inf), (5, 3.0), (6, nan);

SELECT 'NaN comparison';
SELECT count() FROM tab WHERE col = nan;
SELECT count() FROM tab WHERE col <> nan;
SELECT count() FROM tab WHERE col = -nan;
SELECT count() FROM tab WHERE col <> -nan;
SELECT count() FROM tab WHERE isNaN(col);
SELECT count() FROM tab WHERE not isNaN(col);

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

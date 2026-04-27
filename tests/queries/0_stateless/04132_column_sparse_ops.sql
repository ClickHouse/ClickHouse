-- Exercise ColumnSparse operations (filter, expand, permute, index, compare,
-- aggregate dispatch, sort). Triggers the sparse serialization by using
-- mostly-default values with a low ratio_of_defaults_for_sparse_serialization.

DROP TABLE IF EXISTS sparse_ops;
CREATE TABLE sparse_ops (id UInt32, v Int64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;

INSERT INTO sparse_ops
SELECT
    number,
    if(number % 20 = 0, number + 1, 0) AS v,
    if(number % 30 = 0, concat('s', toString(number)), '') AS s
FROM numbers(300);

SELECT '--- parts_columns: which columns chose sparse serialization ---';
SELECT name, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'sparse_ops' AND active
ORDER BY name, serialization_kind;

SELECT '--- aggregate over sparse Int64 column ---';
SELECT count(), countIf(v = 0), countIf(v != 0) FROM sparse_ops;
SELECT sum(v), avg(v), min(v), max(v) FROM sparse_ops;

SELECT '--- aggregate over sparse String column ---';
SELECT countIf(s = ''), countIf(s != '') FROM sparse_ops;

SELECT '--- GROUP BY on sparse column ---';
SELECT v, count() FROM sparse_ops GROUP BY v ORDER BY v LIMIT 5;

SELECT '--- filter (WHERE) on sparse column ---';
SELECT count() FROM sparse_ops WHERE v = 0;
SELECT count() FROM sparse_ops WHERE v > 50;
SELECT count() FROM sparse_ops WHERE s = '';
SELECT count() FROM sparse_ops WHERE s != '';

SELECT '--- ORDER BY sparse column ---';
SELECT v FROM sparse_ops ORDER BY v DESC LIMIT 5;
SELECT v FROM sparse_ops ORDER BY v ASC LIMIT 5;

SELECT '--- SELECT with id filter (reads subset) ---';
SELECT id, v, s FROM sparse_ops WHERE id IN (0, 20, 30, 40) ORDER BY id;

SELECT '--- arithmetic over sparse column ---';
SELECT sum(v * 2), sum(v + 1), sum(if(v > 0, v, 100)) FROM sparse_ops;

SELECT '--- CAST of sparse column ---';
SELECT sum(v::Float64), sum(toString(v)::Int64) FROM sparse_ops;

SELECT '--- JOIN with sparse columns ---';
SELECT sum(r.v) FROM sparse_ops l INNER JOIN sparse_ops r USING id WHERE l.v != 0;

SELECT '--- subquery with DISTINCT over sparse ---';
SELECT count() FROM (SELECT DISTINCT v FROM sparse_ops);

SELECT '--- INSERT SELECT to roundtrip sparse encoding ---';
DROP TABLE IF EXISTS sparse_dst;
CREATE TABLE sparse_dst (id UInt32, v Int64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;
INSERT INTO sparse_dst SELECT * FROM sparse_ops;
SELECT count(), sum(v), countIf(s != '') FROM sparse_dst;

SELECT '--- ALTER: update a few rows (forces materialization) ---';
ALTER TABLE sparse_ops UPDATE v = 999 WHERE id IN (1, 2, 3) SETTINGS mutations_sync = 2;
SELECT count(), sum(v) FROM sparse_ops;

DROP TABLE sparse_ops;
DROP TABLE sparse_dst;

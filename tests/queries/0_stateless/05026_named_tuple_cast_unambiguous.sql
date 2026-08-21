-- Conversions between named tuples must never silently fill elements by default values
-- when the correspondence between elements is ambiguous.
-- https://github.com/ClickHouse/ClickHouse/issues/103527
-- https://github.com/ClickHouse/ClickHouse/issues/70830

SELECT 'Tuples with disjoint sets of element names are converted positionally';
SELECT CAST(CAST((1, 2), 'Tuple(a Int32, b Int32)'), 'Tuple(x Int64, y Int64)');
SELECT accurateCastOrNull(CAST((1, 2), 'Tuple(a Int32, b Int32)'), 'Tuple(x Int32, y Int32)');
SELECT CAST(tuple(x, y) AS Tuple(a Int32, b Int32)) FROM (SELECT 1 AS x, 2 AS y) SETTINGS enable_named_columns_in_function_tuple = 1;

SELECT 'Matching by name still works, extra source elements are dropped';
SELECT CAST(CAST((1, 2), 'Tuple(a Int32, b Int32)'), 'Tuple(b Int64, a Int64)');
SELECT CAST(CAST((1, 2), 'Tuple(a Int32, b Int32)'), 'Tuple(b Int64)');

SELECT 'New elements of the target tuple are filled by default values';
SELECT CAST(CAST((1, 2), 'Tuple(a Int32, b Int32)'), 'Tuple(b Int64, a Int64, c Int64)');

SELECT 'Tuples with common element names are matched by name (schema evolution by ALTER)';
SELECT CAST(CAST((1, 2), 'Tuple(a Int32, b Int32)'), 'Tuple(b Int64, c Int64)');

SELECT 'Disjoint sets of element names with different tuple sizes cannot be converted';
SELECT CAST(CAST((1, 2), 'Tuple(a Int32, b Int32)'), 'Tuple(x Int64, y Int64, z Int64)'); -- { serverError TYPE_MISMATCH }

SELECT 'Inserting the result of function tuple into a differently named tuple keeps the data';
DROP TABLE IF EXISTS src_70830;
DROP TABLE IF EXISTS target_70830;
SET flatten_nested = 0;
CREATE TABLE src_70830 (id UInt64, minmax Nested(min Float64, max Float64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE target_70830 AS src_70830;
INSERT INTO src_70830 VALUES (0, [(-10, 10)]);
INSERT INTO target_70830 SELECT id, groupArray(tuple(bucket_min, bucket_max))
FROM
(
    SELECT id, minmax.1 AS bucket_min, minmax.2 AS bucket_max
    FROM (SELECT id, arrayJoin(minmax) AS minmax FROM src_70830)
)
GROUP BY id
SETTINGS enable_named_columns_in_function_tuple = 1;
SELECT * FROM target_70830;
DROP TABLE src_70830;
DROP TABLE target_70830;

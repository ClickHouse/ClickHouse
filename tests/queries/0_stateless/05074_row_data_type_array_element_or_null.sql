SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_nested;

-- Row cannot be inside Nullable, so arrayElementOrNull on an Array(Row) returns the default row
-- for an out-of-range index, like it does for Array and Map elements, and keeps the Row type.
CREATE TABLE row_nested (id UInt64, a String, b UInt32, c String, combined Array(Row(a String, b UInt32, c String)) ALIAS [(a, b, c), (c, b + 1, a)]) ENGINE = MergeTree ORDER BY id;
INSERT INTO row_nested (id, a, b, c) VALUES (1, 'alpha', 10, 'x'), (2, 'beta', 20, 'y');

SELECT id, arrayElementOrNull(combined, 1) AS e, toTypeName(e) FROM row_nested ORDER BY id;
SELECT id, arrayElementOrNull(combined, -2) AS e, toTypeName(e) FROM row_nested ORDER BY id;
SELECT id, arrayElementOrNull(combined, 100) AS e, toTypeName(e) FROM row_nested ORDER BY id;
SELECT id, arrayElementOrNull(combined, -100) AS e, toTypeName(e) FROM row_nested ORDER BY id;
SELECT id, arrayElementOrNull(combined, id + 1) AS e, toTypeName(e) FROM row_nested ORDER BY id;
SELECT id, arrayElementOrNull(combined, [1, -1, 100]) AS e, toTypeName(e) FROM row_nested ORDER BY id;
SELECT id, arrayElementOrNull(combined, toNullable(1)) AS e, toTypeName(e) FROM row_nested ORDER BY id;
SELECT id, arrayElementOrNull(combined, NULL) AS e, toTypeName(e) FROM row_nested ORDER BY id;

-- arrayElement and the subscript operator agree with it.
SELECT id, arrayElement(combined, 1) AS e, toTypeName(e) FROM row_nested ORDER BY id;
SELECT id, combined[-2] AS e, toTypeName(e), combined[100] AS f, toTypeName(f) FROM row_nested ORDER BY id;
SELECT id, combined[toNullable(1)] AS e, toTypeName(e) FROM row_nested ORDER BY id;

-- The query shape found by the AST fuzzer.
SELECT arrayElementOrNull(combined, -2) FROM row_nested ORDER BY id DESC NULLS LAST WITH FILL;

-- A constant array and a nested Row.
SELECT arrayElementOrNull([(1, 'a')]::Array(Row(x UInt64, y String)), 2) AS e, toTypeName(e);
SELECT arrayElementOrNull([((1, 'a'), 2)]::Array(Tuple(r Row(x UInt64, y String), n UInt8)), 2) AS e, toTypeName(e);

DROP TABLE row_nested;

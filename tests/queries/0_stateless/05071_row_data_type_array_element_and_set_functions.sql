SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_nested;
DROP TABLE IF EXISTS row_set;

-- arrayElement takes an Array(Row) apart element by element, like an Array(Tuple).
CREATE TABLE row_nested (id UInt64, a String, b UInt32, c String, combined Array(Row(a String, b UInt32, c String)) ALIAS [(a, b, c), (c, b + 1, a)]) ENGINE = MergeTree ORDER BY id;
INSERT INTO row_nested (id, a, b, c) VALUES (1, 'alpha', 10, 'x'), (2, 'beta', 20, 'y');

SELECT id, arrayElement(combined, 1) AS e, toTypeName(e) FROM row_nested ORDER BY id;
SELECT id, combined[2], combined[-1], combined[3] FROM row_nested ORDER BY id;
SELECT id, combined[id] FROM row_nested ORDER BY id;
SELECT [(1, 'a')]::Array(Row(x UInt64, y String))[1] AS e, toTypeName(e);
SELECT [[(1, 'a')]]::Array(Array(Row(x UInt64, y String)))[1][1] AS e, toTypeName(e);
SELECT [((1, 'a'), 2)]::Array(Tuple(r Row(x UInt64, y String), n UInt8))[1] AS e, toTypeName(e);

-- The array set functions keep the Row type when every argument has it.
CREATE TABLE row_set (a UInt64, r Row(x UInt64, y String), r2 Row(x UInt64, y String), ar Array(Row(x UInt64, y String))) ENGINE = MergeTree ORDER BY a;
INSERT INTO row_set VALUES (1, (1, 'a'), (1, 'a'), [(1, 'a'), (2, 'b')]), (2, (1, 'b'), (2, 'b'), [(2, 'b'), (3, 'c')]), (3, (0, 'z'), (0, 'z'), []);

SELECT a, arrayIntersect([r], [r2]) AS v, toTypeName(v) FROM row_set ORDER BY a;
SELECT a, arrayIntersect([r]) FROM row_set ORDER BY a;
SELECT a, arraySort(arrayUnion([r], [r2])) FROM row_set ORDER BY a;
SELECT a, arraySort(arraySymmetricDifference([r], [r2])) FROM row_set ORDER BY a;
SELECT a, arrayIntersect([[r]], [[r2]]) FROM row_set ORDER BY a;
SELECT a, arraySort(arrayIntersect(ar, [r, r2])), arraySort(arrayUnion(ar, [r2])), arraySort(arraySymmetricDifference(ar, [r])) FROM row_set ORDER BY a;

DROP TABLE row_nested;
DROP TABLE row_set;

-- `arrayIntersect` decided whether to emit `NULL` from the first argument alone, so a `NULL` present
-- only in the first array ended up in the "intersection" - an element absent from another argument -
-- and swapping the arguments of this commutative function changed the result.

SELECT 'NULL only in the first array';
SELECT arrayIntersect(CAST([NULL, 1] AS Array(Nullable(UInt8))), CAST([1, 2] AS Array(Nullable(UInt8))));
SELECT arrayIntersect(CAST([1, 2] AS Array(Nullable(UInt8))), CAST([NULL, 1] AS Array(Nullable(UInt8))));
SELECT arrayIntersect(CAST(['a', NULL] AS Array(Nullable(String))), CAST(['a', 'b'] AS Array(Nullable(String))));
SELECT arrayIntersect(CAST([NULL, 1] AS Array(Nullable(UInt8))), CAST([1, 2] AS Array(Nullable(UInt8))), CAST([1, 3] AS Array(Nullable(UInt8))));

SELECT 'NULL in every array';
SELECT arrayIntersect(CAST([NULL, 1] AS Array(Nullable(UInt8))), CAST([NULL, 2] AS Array(Nullable(UInt8))));
SELECT arrayIntersect(CAST([NULL, 1] AS Array(Nullable(UInt8))), CAST([NULL, 1] AS Array(Nullable(UInt8))), CAST([NULL, 1] AS Array(Nullable(UInt8))));
SELECT arrayIntersect(CAST(['a', NULL] AS Array(Nullable(String))), CAST([NULL, 'a'] AS Array(Nullable(String))));

SELECT 'the value under a NULL is not a member of the array';
SELECT arrayIntersect(CAST([NULL, 5] AS Array(Nullable(UInt8))), CAST([0, 5] AS Array(Nullable(UInt8))));

SELECT 'commutative over a table';
DROP TABLE IF EXISTS t_array_intersect_null;
CREATE TABLE t_array_intersect_null (id UInt32, a Array(Nullable(UInt8)), b Array(Nullable(UInt8))) ENGINE = Memory;
INSERT INTO t_array_intersect_null VALUES (1, [NULL, 1], [1, 2]), (2, [1, 2], [NULL, 1]), (3, [NULL, 1], [NULL, 2]);
SELECT id, arrayIntersect(a, b) AS ab, arrayIntersect(b, a) AS ba, ab = ba AS commutative
FROM t_array_intersect_null ORDER BY id;
DROP TABLE t_array_intersect_null;

SELECT 'the other set functions are unchanged';
SELECT arrayUnion(CAST([NULL, 1] AS Array(Nullable(UInt8))), CAST([1, 2] AS Array(Nullable(UInt8))));
SELECT arraySymmetricDifference(CAST([NULL, 1] AS Array(Nullable(UInt8))), CAST([1, 2] AS Array(Nullable(UInt8))));

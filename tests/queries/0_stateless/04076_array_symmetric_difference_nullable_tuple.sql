-- Regression test: arraySymmetricDifference/arrayIntersect/arrayUnion with
-- nullable complex types (tuples, arrays) caused a logical error due to
-- serialization/deserialization mismatch when the result column was incorrectly
-- created as Nullable.

SELECT arraySymmetricDifference([(NULL, ['', ''])], [arraySymmetricDifference(NULL, NULL), (0, ['a', 'b']), (NULL, ['c'])]);
SELECT arraySymmetricDifference([(1, 'a'), (2, 'b')], [(2, 'b'), (3, 'c')]);
SELECT arrayIntersect([(NULL, ['a', 'b']), (1, ['c'])], [(1, ['c']), (NULL, ['a', 'b'])]);
SELECT arrayUnion([(NULL, ['a'])], [(1, ['b']), (NULL, ['a'])]);

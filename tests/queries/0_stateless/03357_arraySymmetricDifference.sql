SELECT 'Negative tests';
SELECT arraySymmetricDifference(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arraySymmetricDifference(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arraySymmetricDifference(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arraySymmetricDifference(1, [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arraySymmetricDifference([1, 2], 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Const arguments';
SELECT arraySort(arraySymmetricDifference([]));
SELECT arraySort(arraySymmetricDifference([1, 2]));
SELECT arraySort(arraySymmetricDifference([1, 2], [1, 3]));
SELECT arraySort(arraySymmetricDifference(['a', 'b'], ['a', 'c']));
SELECT arraySort(arraySymmetricDifference([1, NULL], [1, 3]));
SELECT arraySort(arraySymmetricDifference([1, NULL], [NULL, 3]));
SELECT arraySort(arraySymmetricDifference([1, 1], [1, 1]));
SELECT arraySort(arraySymmetricDifference([1, 2], [1, 2]));
SELECT arraySort(arraySymmetricDifference([1, 2], [1, 2], [1, 2]));
SELECT arraySort(arraySymmetricDifference([1, 2], [1, 2], [1, 3]));

SELECT toTypeName(arraySymmetricDifference([(1, ['a', 'b']), (Null, ['c'])], [(2, ['c', Null]), (1, ['a', 'b'])]));

SELECT 'Non-const arguments';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id                              UInt8,
    array_tuple1                    Array( Tuple(Nullable(Int32),Array(String)) ),
    array_tuple2                    Array( Tuple(Nullable(Int32),Array(String)) ),
    array_lowcardinality_string1    Array( LowCardinality(String) ),
    array_lowcardinality_string2    Array( LowCardinality(String) )
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, [(1, ['a', 'b']), (Null, ['c'])], [(2, ['c', Null]), (1, ['a', 'b'])], ['a', 'b', 'c'], ['c', 'a', 'b']);
INSERT INTO tab VALUES (2, [(2, ['d', 'e']), (Null, ['f'])], [(3, ['g', Null]), (2, ['d', 'e'])], ['d', 'e', 'f'], ['f', 'd', 'e']);
INSERT INTO tab VALUES (3, [(3, ['g', 'h']), (Null, ['i'])], [(4, ['j', Null]), (3, ['g', 'h'])], ['g', 'h', 'i'], ['i', 'g', 'h']);
INSERT INTO tab VALUES (4, [(4, ['j', 'k']), (Null, ['l'])], [(5, ['m', Null]), (4, ['j', 'k'])], ['j', 'k', 'l'], ['l', 'j', 'k']);

--
SELECT arraySort(arraySymmetricDifference(array_tuple1, array_tuple2)) FROM tab ORDER BY id;
SELECT arraySort(arraySymmetricDifference(array_lowcardinality_string1, array_lowcardinality_string2)) FROM tab ORDER BY id;

DROP TABLE tab;

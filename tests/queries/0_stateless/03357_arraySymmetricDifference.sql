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
WITH
    materialize([(1, ['a', 'b']), (NULL, ['c'])]) AS f,
    materialize([(2, ['c', NULL]), (1, ['a', 'b'])]) AS s
SELECT arraySort(arraySymmetricDifference(f, s));
WITH
    materialize([(1, ['a', 'b']::Array(LowCardinality(String))), (NULL, ['c']::Array(LowCardinality(String)))]) AS f,
    materialize([(2, ['c', NULL]::Array(LowCardinality(Nullable(String)))), (1, ['a', 'b']::Array(LowCardinality(String)))]) AS s
SELECT arraySort(arraySymmetricDifference(f, s));

-- Table with batch inserts
DROP TABLE IF EXISTS test_arraySymmetricDifference;
CREATE TABLE test_arraySymmetricDifference
(
    `id` Int8,
    `arr1` Array(String),
    `arr2` Array(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_arraySymmetricDifference
VALUES
(1, ['1'], ['2']),
(2, ['2'], ['2']),
(3, ['3'], ['3', '2']),
(4, ['4'], ['1']),
(5, ['5'], []),
(6, ['6', '4'], ['5', '6']),
(7, ['7', '0'], []),
(8, ['8', '9', '10'], []),
(9, ['9'], ['-1']),
(10, ['10'], ['5']);

SELECT
	ta.id AS id,
    arraySort(arraySymmetricDifference(ta.arr1, ta.arr2)) AS symmetricDifference
FROM test_arraySymmetricDifference ta;

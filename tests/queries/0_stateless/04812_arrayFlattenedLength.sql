SELECT arrayFlattenedLength([[1, 2], [3, 4]]);
SELECT arrayFlattenedLength([[[1]], [[2], [3]]]);
SELECT arrayFlattenedLength([1, 2, 3]);
SELECT arrayFlattenedLength([]);
SELECT arrayFlattenedLength([[], []]);
SELECT arrayFlattenedLength([[[], [1, 2, 3]], []]);
SELECT arrayFlattenedLength([['a'], ['b', 'c']]);
SELECT arrayFlattenedLength([[NULL, 1], [NULL]]);
SELECT arrayFlattenedLength([[(1, 'a')], [(2, 'b'), (3, 'c')]]);
SELECT arrayFlattenedLength(materialize([[1, 2], [3, 4]]));
SELECT arrayFlattenedLength(CAST(NULL, 'Nullable(Nothing)'));

-- The result is UInt64, and Nullable only for a Nullable argument.
SELECT toTypeName(arrayFlattenedLength([[1]])), toTypeName(arrayFlattenedLength(materialize([[1]]))), toTypeName(arrayFlattenedLength(NULL));

-- Deep nesting of a non-constant argument: the offsets of every level are folded into a single buffer.
SELECT arrayFlattenedLength(materialize([[[[1, 2], [3]], [[4]]], [[[5]]]]));
SELECT arrayFlattenedLength(materialize([[[[[[[[[[1, 2, 3]]]]]]]]]]));

-- Several rows, with empty arrays at every level.
SELECT arrayFlattenedLength(arr), length(arrayFlatten(arr))
FROM values('arr Array(Array(Array(UInt8)))',
    ([[[1, 2], []], [[3]]]), ([]), ([[]]), ([[], []]), ([[[]]]), ([[[1]], [], [[2], [3, 4]]]));

-- Element types that the descent has to look through.
SELECT arrayFlattenedLength(materialize(CAST([['a'], ['b', 'c']], 'Array(Array(LowCardinality(String)))')));
SELECT arrayFlattenedLength(materialize(CAST([[NULL, 1], [NULL]], 'Array(Array(Nullable(UInt8)))')));

-- Only Array nesting is followed: the descent stops at Tuple, Map and Dynamic elements, even when they hold arrays.
SELECT arrayFlattenedLength([tuple([1, 2]), tuple([3])]), length(arrayFlatten([tuple([1, 2]), tuple([3])]));
SELECT arrayFlattenedLength(materialize([map('a', [1, 2, 3])]));
SELECT arrayFlattenedLength(a), length(arrayFlatten(a)) FROM (SELECT arrayMap(x -> x::Dynamic, [[1, 2], [3, 4]]) AS a);

-- A Dynamic or Variant argument is unwrapped by the default implementation, which makes the result Nullable.
SELECT arrayFlattenedLength(d), toTypeName(arrayFlattenedLength(d))
FROM values('d Dynamic', ([[1, 2], [3, 4]]), ('abc'), ([[[1]], [[2]]]))
SETTINGS dynamic_throw_on_type_mismatch = 0;
SELECT arrayFlattenedLength(v), toTypeName(arrayFlattenedLength(v)) FROM (SELECT CAST([[1, 2], [3]], 'Variant(Array(Array(UInt8)), String)') AS v);

-- An empty input must not touch the offsets of a nested column.
SELECT arrayFlattenedLength(arr) FROM values('arr Array(Array(UInt8))', ([[1]])) WHERE 0;

SELECT arrayFlattenedLength(map(1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFlattenedLength('abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayFlattenedLength(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayFlattenedLength([1], [2]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Compare against the flatten-then-count workaround on non-trivial data, over many small blocks.
SELECT countIf(arrayFlattenedLength(arr) != length(arrayFlatten(arr))), sum(arrayFlattenedLength(arr))
FROM
(
    SELECT arrayMap(i -> range(i % 5), range(number % 7)) AS arr
    FROM numbers(1000)
)
SETTINGS max_block_size = 7;

-- The same for a three-dimensional array, and with empty arrays interleaved.
SELECT countIf(arrayFlattenedLength(arr) != length(arrayFlatten(arr))), sum(arrayFlattenedLength(arr))
FROM
(
    SELECT arrayMap(i -> arrayMap(j -> range(j % 3), range(i % 4)), range(number % 6)) AS arr
    FROM numbers(1000)
);

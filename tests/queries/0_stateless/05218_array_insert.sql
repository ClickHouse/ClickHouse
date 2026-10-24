SELECT arrayInsert([1, 2, 3], 1, 0);
SELECT arrayInsert([1, 2, 3], 2, 0);
SELECT arrayInsert([1, 2, 3], 4, 0);
SELECT arrayInsert([], 1, 7);

SELECT arrayInsert([1, 2, 3], -4, 0);
SELECT arrayInsert([1, 2, 3], -3, 0);
SELECT arrayInsert([1, 2, 3], -2, 0);
SELECT arrayInsert([1, 2, 3], -1, 0);
SELECT arrayInsert([], -1, 7);

SELECT arrayInsert(arr, pos, value)
FROM values('arr Array(Int32), pos Int64, value Int32',
    ([1, 2, 3], 1, 9),
    ([4, 5], -1, 8),
    ([], 1, 7));

SELECT arrayInsert([1, 2, 3], pos, 9)
FROM values('pos Int64', 2, -1);

SELECT arrayInsert(arr, 2, 9)
FROM values('arr Array(Int32)', [1, 2, 3], [6], [4, 5]);

SELECT arrayInsert(arr, pos, 9)
FROM values('arr Array(Int32), pos UInt64',
    ([1, 2], 2),
    ([], 1));

SELECT arrayInsert(arr, pos, 9)
FROM values('arr Array(Int32), pos Int64',
    ([1, 2, 3], 5),
    ([4, 5], -4)); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT arrayInsert(arr, pos, 9)
FROM values('arr Array(Int32), pos UInt64',
    ([1, 2], 4),
    ([], 2)); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT arrayInsert([1, 2], 1, NULL);
SELECT arrayInsert(['a', 'b'], 2, 'x');
SELECT arrayInsert([(1, 'a'), (2, 'b')], 2, (9, 'x'));
SELECT arrayInsert([[1, 2], [3]], 2, [9, 10]);
SELECT toTypeName(arrayInsert([toUInt8(1)], 1, toUInt16(2)));
SELECT arrayInsert([1, 2, 3], toUInt64(4), 0);

SET allow_suspicious_low_cardinality_types = 1;

SELECT toTypeName(arrayInsert(['a', 'b']::Array(LowCardinality(String)), 2, toLowCardinality('x')));

SELECT inserted[2], length(inserted)
FROM
(
    SELECT arrayInsert(
        range(number, number + 300)::Array(LowCardinality(UInt64)),
        2,
        toLowCardinality(number + 1000)) AS inserted
    FROM numbers(1)
);

SELECT count(), sum(length(inserted)), countIf(inserted[1] = 'array'), countIf(inserted[2] = toString(number)),
    any(toTypeName(inserted))
FROM
(
    SELECT
        number,
        arrayInsert([toLowCardinality('array')], 2, toLowCardinality(toString(number))) AS inserted
    FROM numbers(300)
);

-- LowCardinality position arguments must act like their native integer types.
SELECT arrayInsert([1, 2, 3], toLowCardinality(toUInt8(2)), 9);
SELECT arrayInsert([1, 2, 3], toLowCardinality(pos), 9)
FROM values('pos UInt8', 1, 4);
SELECT arrayInsert([1, 2, 3], toLowCardinality(pos), 9)
FROM values('pos Int64', -4, -1);

-- The result dictionary must support values from both sides, even across index width changes.
SELECT count(), sum(length(inserted)), countIf(inserted[2] = toString(number + 250)), any(toTypeName(inserted))
FROM
(
    SELECT
        number,
        arrayInsert(
            arrayMap(i -> toString(i), range(250))::Array(LowCardinality(String)),
            2,
            toLowCardinality(toString(number + 250))) AS inserted
    FROM numbers(300)
);

SELECT arrayInsert([1, 2, 3], 0, 9); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arrayInsert([1, 2, 3], toLowCardinality(toUInt8(0)), 9); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arrayInsert([1, 2, 3], 5, 9); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arrayInsert([1, 2, 3], -5, 9); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arrayInsert([1, 2, 3], toUInt64(18446744073709551615), 9); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arrayInsert([1, 2, 3], toInt64(-9223372036854775808), 9); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arrayInsert([1, 2, 3], toFloat64(1), 9); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayInsert([1, 2, 3], NULL, 9); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayInsert([1, 2, 3], toNullable(toUInt8(1)), 9); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Constant signed and unsigned positions with variable arrays.
SELECT arrayInsert(arr, -2, 9), arrayInsert(arr, toUInt64(2), 9)
FROM values('arr Array(Int32)', [1], [1, 2, 3]);

-- Constant array with unsigned positions and per-row inserted values.
SELECT arrayInsert([1, 2, 3], pos, value)
FROM values('pos UInt64, value Int32', (1, 7), (2, 8), (4, 9));

-- Extreme positions must also be rejected by the dynamic path.
SELECT arrayInsert([1, 2, 3], materialize(toUInt64(18446744073709551615)), 9); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arrayInsert([1, 2, 3], materialize(toInt64(-9223372036854775808)), 9); -- { serverError ARGUMENT_OUT_OF_BOUND }

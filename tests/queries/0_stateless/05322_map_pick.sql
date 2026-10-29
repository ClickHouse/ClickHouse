SELECT
    mapPick(map('a', 1, 'b', 2, 'c', 3), 'a', 'c'),
    mapPick(map('a', 1, 'b', 2), 'missing'),
    mapPick(map('a', 1), 'a'),
    mapPick(mapFromArrays(emptyArrayString(), emptyArrayUInt8()), 'a')
FORMAT TabSeparatedRaw;

SELECT mapPick(map(), 'a'), toTypeName(mapPick(map(), 'a')) FORMAT TabSeparatedRaw;

SELECT mapPick(map('a', 1, 'a', 2, 'b', 3), 'a') FORMAT TabSeparatedRaw;

SELECT mapPick(map('a', 1, 'b', 2), 'a', 'a') FORMAT TabSeparatedRaw;

SELECT mapPick(map('b', 2, 'a', 1, 'c', 3), 'a', 'b') FORMAT TabSeparatedRaw;

SELECT mapPick(
    map(toInt64(1), 'one', toInt64(2), 'two'),
    toUInt8(1),
    toInt32(2))
FORMAT TabSeparatedRaw;

SELECT
    mapPick(map('a', 1, 'b', 2), CAST('a', 'Nullable(String)')),
    mapPick(map('a', 1, 'b', 2), CAST(NULL, 'Nullable(String)'))
FORMAT TabSeparatedRaw;

SELECT
    mapPick(map('a', 1, 'b', 2), NULL),
    mapPick(map('a', 1, 'b', 2), 'a', NULL)
FORMAT TabSeparatedRaw;

SELECT mapPick(map('a', 1, 'b', 2), CAST('a', 'LowCardinality(String)')) FORMAT TabSeparatedRaw;

SELECT number, mapPick(
    map('a', number, 'b', number + 1),
    if(number = 0, 'a', 'b'))
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT number, mapPick(
    map('a', number, 'b', number + 1),
    'a')
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT number, mapPick(
    if(
        number = 0,
        mapFromArrays(emptyArrayString(), emptyArrayUInt64()),
        map('a', toUInt64(1), 'b', toUInt64(2))),
    if(number = 0, 'missing', 'a'))
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT number, mapPick(
    map('a', 1, 'b', 2),
    if(number = 0, 'a', 'b'))
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT number, mapPick(
    map('a', number, 'b', number + 1, 'c', number + 2),
    if(number = 0, 'a', 'b'),
    if(number = 0, 'c', 'missing'))
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

WITH mapPick(
    map(tuple(CAST(NULL, 'Nullable(UInt8)'), toUInt8(1)), 'null', tuple(toNullable(toUInt8(5)), toUInt8(1)), 'five'),
    tuple(CAST(NULL, 'Nullable(UInt8)'), toUInt8(1))) AS picked
SELECT
    mapContainsKey(picked, tuple(CAST(NULL, 'Nullable(UInt8)'), toUInt8(1))),
    length(mapKeys(picked))
FORMAT TabSeparatedRaw;

SELECT mapPick(
    map('a', CAST(NULL, 'Nullable(UInt8)'), 'b', toNullable(toUInt8(2))),
    'b')
FORMAT TabSeparatedRaw;

SELECT toTypeName(
    mapPick(
        CAST(map('a', 'x', 'b', 'y'), 'Map(LowCardinality(String), LowCardinality(String))'),
        'a'))
FORMAT TabSeparatedRaw;

SELECT mapPick(
    CAST(map('a', 'x', 'b', 'y'), 'Map(LowCardinality(String), LowCardinality(String))'),
    'a')
FORMAT TabSeparatedRaw;

WITH mapPick(map(nan, 'nan', 1.5, 'one-five'), nan) AS picked
SELECT
    isNaN(mapKeys(picked)[1]),
    mapValues(picked)[1],
    length(mapKeys(picked))
FORMAT TabSeparatedRaw;

WITH mapPick(
    map(tuple(nan, toUInt8(1)), 'nan', tuple(1.5, toUInt8(1)), 'one-five'),
    tuple(nan, toUInt8(1))) AS picked
SELECT
    isNaN(tupleElement(mapKeys(picked)[1], 1)),
    tupleElement(mapKeys(picked)[1], 2),
    mapValues(picked)[1],
    length(mapKeys(picked))
FORMAT TabSeparatedRaw;

SELECT mapPick(map([1, 2], 'first', [3], 'second'), [1, 2]) FORMAT TabSeparatedRaw;

SELECT mapPick(
    map([toUInt64(1)], 'one', [toUInt64(2)], 'two'),
    [toInt64(1)])
FORMAT TabSeparatedRaw;

SELECT mapPick(
    map(tuple(nan, toUInt64(1)), 'nan', tuple(1.5, toUInt64(1)), 'one-five'),
    tuple(nan, toInt64(1)))
FORMAT TabSeparatedRaw;

-- Both selection modes must handle comparable array keys without a common supertype.
SELECT
    mapPick(map([toUInt64(1)], 'one', [toUInt64(2)], 'two'), [toInt64(1)]),
    mapRemove(map([toUInt64(1)], 'one', [toUInt64(2)], 'two'), [toInt64(1)])
FORMAT TabSeparatedRaw;

SELECT mapPick(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapPick(map('a', 1)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapPick([1, 2], 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapPick(map('a', 1), [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, NO_COMMON_TYPE }

SET print_pretty_type_names = 0;

SELECT mapFromEntries([('a', toUInt8(1)), ('b', toUInt8(2))]) FORMAT TabSeparatedRaw;
SELECT toTypeName(mapFromEntries([('a', toUInt8(1))])) FORMAT TabSeparatedRaw;
SELECT MAP_FROM_ENTRIES([('a', toUInt8(1))]) FORMAT TabSeparatedRaw;

SELECT number, mapFromEntries([('a', toUInt8(1))])
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT mapEntries(mapFromEntries([('a', 1), ('a', 2), ('b', 3)])) FORMAT TabSeparatedRaw;

SELECT mapFromEntries(CAST([], 'Array(Tuple(String, UInt8))')) FORMAT TabSeparatedRaw;
SELECT mapFromEntries([]), toTypeName(mapFromEntries([])) FORMAT TabSeparatedRaw;

SELECT mapFromEntries(arrayZip(['a', 'b'], [toUInt8(1), toUInt8(2)])) FORMAT TabSeparatedRaw;
SELECT mapFromEntries(CAST([('a', 1), ('b', 2)], 'Array(Tuple(foo String, bar UInt8))')) FORMAT TabSeparatedRaw;

SELECT mapFromEntries(CAST([('a', 1), ('b', 2)], 'Array(Tuple(Nullable(String), UInt8))')) FORMAT TabSeparatedRaw;
SELECT toTypeName(mapFromEntries(CAST([('a', 1)], 'Array(Tuple(Nullable(String), UInt8))'))) FORMAT TabSeparatedRaw;
SELECT
    mapFromEntries(CAST([], 'Array(Tuple(Nullable(String), UInt8))')),
    toTypeName(mapFromEntries(CAST([], 'Array(Tuple(Nullable(String), UInt8))')))
FORMAT TabSeparatedRaw;

SELECT
    mapFromEntries([
        (toLowCardinality(toNullable('a')), toUInt8(1)),
        (toLowCardinality(toNullable('b')), toUInt8(2))
    ]),
    toTypeName(mapFromEntries([
        (toLowCardinality(toNullable('a')), toUInt8(1)),
        (toLowCardinality(toNullable('b')), toUInt8(2))
    ]))
GROUP BY 1, 2
FORMAT TabSeparatedRaw;

SELECT
    number,
    mapFromEntries(CAST([(toString(number), number)], 'Array(Tuple(LowCardinality(Nullable(String)), UInt64))'))
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT mapFromEntries(CAST([('a', 1), ('b', NULL)], 'Array(Tuple(String, Nullable(UInt8)))')) FORMAT TabSeparatedRaw;

SELECT
    mapFromEntries(CAST([('a', 'x'), ('b', 'y')], 'Array(Tuple(LowCardinality(String), LowCardinality(String)))')),
    toTypeName(mapFromEntries(CAST([('a', 'x')], 'Array(Tuple(LowCardinality(String), LowCardinality(String)))')))
FORMAT TabSeparatedRaw;

SELECT mapFromEntries([('a', [1, 2]), ('b', [3])]) FORMAT TabSeparatedRaw;

SELECT number, mapFromEntries([('a', number), ('b', number + 1)])
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

SELECT mapEntries(mapFromEntries(mapEntries(map('a', 1, 'a', 2)))) FORMAT TabSeparatedRaw;

SELECT mapFromEntries(CAST([('a', 1), (NULL, 2)], 'Array(Tuple(Nullable(String), UInt8))')); -- { serverError BAD_ARGUMENTS }

SELECT mapFromEntries([
    (toLowCardinality(toNullable('a')), toUInt8(1)),
    (toLowCardinality(CAST(NULL AS Nullable(String))), toUInt8(2))
]) GROUP BY 1; -- { serverError BAD_ARGUMENTS }

SELECT mapFromEntries([1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFromEntries([tuple(1)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFromEntries([tuple(1, 2, 3)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFromEntries(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFromEntries(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapFromEntries([('a', 1)], [('b', 2)]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT mapFromEntries(CAST([('a', 1), ('b', 2)], 'Array(Nullable(Tuple(String, UInt8)))')) FORMAT TabSeparatedRaw;
SELECT toTypeName(mapFromEntries(CAST([('a', 1)], 'Array(Nullable(Tuple(String, UInt8)))'))) FORMAT TabSeparatedRaw;
SELECT
    mapFromEntries(CAST([], 'Array(Nullable(Tuple(String, UInt8)))')),
    toTypeName(mapFromEntries(CAST([], 'Array(Nullable(Tuple(String, UInt8)))')))
FORMAT TabSeparatedRaw;

WITH CAST([('a', 1), ('b', 2)], 'Array(Nullable(Tuple(String, UInt8)))') AS entries
SELECT
    mapEntries(mapFromEntries(entries)) =
        mapEntries(mapFromArrays(arrayMap(e -> tupleElement(e, 1), entries), arrayMap(e -> tupleElement(e, 2), entries))),
    toTypeName(mapFromEntries(entries)) =
        toTypeName(mapFromArrays(arrayMap(e -> tupleElement(e, 1), entries), arrayMap(e -> tupleElement(e, 2), entries)))
FORMAT TabSeparatedRaw;

-- arrayMap removes nested LowCardinality during the old rewrite, but mapFromEntries preserves it.
WITH CAST([('a', 'x')], 'Array(Nullable(Tuple(String, LowCardinality(String))))') AS entries
SELECT
    mapEntries(mapFromEntries(entries)) =
        mapEntries(mapFromArrays(arrayMap(e -> tupleElement(e, 1), entries), arrayMap(e -> tupleElement(e, 2), entries))),
    toTypeName(mapFromEntries(entries))
FORMAT TabSeparatedRaw;

SELECT
    mapFromEntries(CAST([('a', [1, 2]), ('b', [3])], 'Array(Nullable(Tuple(String, Array(UInt8))))')),
    toTypeName(mapFromEntries(CAST([('a', [1, 2])], 'Array(Nullable(Tuple(String, Array(UInt8))))')))
FORMAT TabSeparatedRaw;

SELECT mapFromEntries(CAST([('a', 1), NULL], 'Array(Nullable(Tuple(String, UInt8)))')); -- { serverError BAD_ARGUMENTS }
SELECT mapFromEntries(CAST([('a', 1), (NULL, 2)], 'Array(Nullable(Tuple(Nullable(String), UInt8)))')); -- { serverError BAD_ARGUMENTS }

-- A nullable tuple may also contain a nullable value.
WITH CAST([('a', 1), ('b', NULL)], 'Array(Nullable(Tuple(String, Nullable(UInt8))))') AS entries
SELECT
    mapFromEntries(entries),
    toTypeName(mapFromEntries(entries))
FORMAT TabSeparatedRaw;

-- Exercise offsets and null maps for non-constant arrays across multiple rows.
SELECT
    number,
    mapFromEntries(CAST(
        [(toString(number), toUInt8(number)), ('fixed', toUInt8(42))],
        'Array(Nullable(Tuple(String, UInt8)))'))
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

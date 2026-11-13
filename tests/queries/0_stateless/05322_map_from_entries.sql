SET print_pretty_type_names = 0;

-- Basic construction, type inference, aliases, duplicate keys and empty arrays.
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

-- Named tuples and nullable or LowCardinality key/value types.
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

-- NULL keys and invalid function arguments must fail.
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

-- Non-NULL nullable tuples retain the behavior of the former Trino arrayMap rewrite.
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

-- Reject NULL tuple entries even for key types that cannot become Nullable.
SELECT mapFromEntries(CAST([([1], 1), NULL], 'Array(Nullable(Tuple(Array(UInt8), UInt8)))')); -- { serverError BAD_ARGUMENTS }
SELECT mapFromEntries(CAST([((1, 2), 3), NULL], 'Array(Nullable(Tuple(Tuple(UInt8, UInt8), UInt8)))'))
    SETTINGS allow_nullable_tuple_in_extracted_subcolumns = 0; -- { serverError BAD_ARGUMENTS }
SELECT mapFromEntries(CAST([((1, 2), 3), NULL], 'Array(Nullable(Tuple(Tuple(UInt8, UInt8), UInt8)))'))
    SETTINGS allow_nullable_tuple_in_extracted_subcolumns = 1; -- { serverError BAD_ARGUMENTS }
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

-- Nullable tuple values keep LowCardinality and actual NULL values.
WITH CAST([('a', 'x'), ('b', NULL)], 'Array(Nullable(Tuple(String, LowCardinality(Nullable(String)))))') AS entries
SELECT
    mapEntries(mapFromEntries(entries)) =
        mapEntries(mapFromArrays(arrayMap(e -> tupleElement(e, 1), entries), arrayMap(e -> tupleElement(e, 2), entries))),
    toTypeName(mapFromEntries(entries)) = 'Map(String, LowCardinality(Nullable(String)))'
FORMAT TabSeparatedRaw;

-- Array keys/values cannot be Nullable, but valid nullable tuple entries remain supported.
WITH CAST([([number], [number + 1])], 'Array(Nullable(Tuple(Array(UInt64), Array(UInt64))))') AS entries
SELECT
    number,
    mapEntries(mapFromEntries(entries)) =
        mapEntries(mapFromArrays(arrayMap(e -> tupleElement(e, 1), entries), arrayMap(e -> tupleElement(e, 2), entries)))
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

-- Typed empty arrays preserve their inferred key and value types.
SELECT toTypeName(mapFromEntries(CAST([], 'Array(Nullable(Tuple(Array(UInt8), Array(UInt8))))'))) =
    'Map(Array(UInt8), Array(UInt8))' FORMAT TabSeparatedRaw;
-- Verify array offsets when one row has no entries.
SELECT
    number,
    mapFromEntries(arrayFilter(entry -> number != 0, [('a', toUInt8(1))]))
FROM numbers(2)
ORDER BY number
FORMAT TabSeparatedRaw;

-- NULL entries are invalid even when both fields of the tuple are nullable.
SELECT mapFromEntries(CAST([NULL], 'Array(Nullable(Tuple(Nullable(String), Nullable(UInt8))))')); -- { serverError BAD_ARGUMENTS }

-- Dynamic and Variant keys can contain NULL without Nullable wrappers.
SELECT length(mapKeys(mapFromEntries(CAST([('a', 1)], 'Array(Tuple(Dynamic, UInt8))'))))
SETTINGS allow_experimental_dynamic_type = 1
FORMAT TabSeparatedRaw;
SELECT mapFromEntries(CAST([('a', 1), (NULL, 2)], 'Array(Tuple(Dynamic, UInt8))'))
SETTINGS allow_experimental_dynamic_type = 1; -- { serverError BAD_ARGUMENTS }

SELECT length(mapKeys(mapFromEntries(CAST([('a', 1)], 'Array(Tuple(Variant(String, UInt64), UInt8))'))))
SETTINGS allow_experimental_variant_type = 1
FORMAT TabSeparatedRaw;
SELECT mapFromEntries(CAST([('a', 1), (NULL, 2)], 'Array(Tuple(Variant(String, UInt64), UInt8))'))
SETTINGS allow_experimental_variant_type = 1; -- { serverError BAD_ARGUMENTS }

-- Throwing NULL-entry and NULL-key checks must not inspect rows removed by lazy ARRAY JOIN.
SELECT mapFromEntries(entries)
FROM
(
    SELECT entries
    FROM
    (
        SELECT
            if(number = 0,
                CAST([('a', number)], 'Array(Nullable(Tuple(String, UInt64)))'),
                CAST([NULL], 'Array(Nullable(Tuple(String, UInt64)))')) AS entries,
            if(number = 0, [1, 2, 3], []) AS r
        FROM numbers(2)
    ) ARRAY JOIN r
    LIMIT 3
)
SETTINGS enable_lazy_columns_replication = 0, max_threads = 1, enable_parallel_replicas = 0, compile_expressions = 0
FORMAT TabSeparatedRaw;

SELECT mapFromEntries(entries)
FROM
(
    SELECT entries
    FROM
    (
        SELECT
            if(number = 0,
                CAST([('a', number)], 'Array(Nullable(Tuple(String, UInt64)))'),
                CAST([NULL], 'Array(Nullable(Tuple(String, UInt64)))')) AS entries,
            if(number = 0, [1, 2, 3], []) AS r
        FROM numbers(2)
    ) ARRAY JOIN r
    LIMIT 3
)
SETTINGS enable_lazy_columns_replication = 1, max_threads = 1, enable_parallel_replicas = 0, compile_expressions = 0
FORMAT TabSeparatedRaw;

-- The same holds for nullable keys from unused rows.
SELECT mapFromEntries(entries)
FROM
(
    SELECT entries
    FROM
    (
        SELECT
            if(number = 0,
                CAST([('a', number)], 'Array(Tuple(Nullable(String), UInt64))'),
                CAST([(NULL, number)], 'Array(Tuple(Nullable(String), UInt64))')) AS entries,
            if(number = 0, [1, 2, 3], []) AS r
        FROM numbers(2)
    ) ARRAY JOIN r
    LIMIT 3
)
SETTINGS enable_lazy_columns_replication = 0, max_threads = 1, enable_parallel_replicas = 0, compile_expressions = 0
FORMAT TabSeparatedRaw;

SELECT mapFromEntries(entries)
FROM
(
    SELECT entries
    FROM
    (
        SELECT
            if(number = 0,
                CAST([('a', number)], 'Array(Tuple(Nullable(String), UInt64))'),
                CAST([(NULL, number)], 'Array(Tuple(Nullable(String), UInt64))')) AS entries,
            if(number = 0, [1, 2, 3], []) AS r
        FROM numbers(2)
    ) ARRAY JOIN r
    LIMIT 3
)
SETTINGS enable_lazy_columns_replication = 1, max_threads = 1, enable_parallel_replicas = 0, compile_expressions = 0
FORMAT TabSeparatedRaw;

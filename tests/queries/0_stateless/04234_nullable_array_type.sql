DROP TABLE IF EXISTS nullable_array_type;

CREATE TABLE nullable_array_type_bad (a Nullable(Array(Int32))) ENGINE = Memory; -- {serverError ILLEGAL_COLUMN}

SET allow_experimental_nullable_array_type = 1;

CREATE TABLE nullable_array_type
(
    id UInt8,
    a Nullable(Array(Int32))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO nullable_array_type VALUES (1, NULL), (2, []), (3, [1, 2]);

SELECT throwIf(groupArray(isNull(a)) != [1, 0, 0])
FROM (SELECT a FROM nullable_array_type ORDER BY id)
FORMAT Null;

SELECT throwIf(groupArray(isNull(length(a))) != [1, 0, 0])
FROM (SELECT a FROM nullable_array_type ORDER BY id)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(length(a), 99)) != [99, 0, 2])
FROM (SELECT a FROM nullable_array_type ORDER BY id)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(empty(a), 99)) != [99, 1, 0])
FROM (SELECT a FROM nullable_array_type ORDER BY id)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayElement(a, 1), 99)) != [99, 0, 1])
FROM (SELECT a FROM nullable_array_type ORDER BY id)
FORMAT Null;

SELECT throwIf(groupArray(isNull(arrayMap(x -> x + 1, a))) != [1, 0, 0])
FROM (SELECT a FROM nullable_array_type ORDER BY id)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayElementOrNull(a, -1), 99)) != [99, 99, 2])
FROM (SELECT a FROM nullable_array_type ORDER BY id)
FORMAT Null;

SELECT throwIf(toTypeName(a) != 'Nullable(Array(Int32))')
FROM nullable_array_type
FORMAT Null;

DROP TABLE nullable_array_type;

SELECT arrayElementOrNull(a, 'k')
FROM system.one
ARRAY JOIN CAST([map('k', tuple([1, 2])), map()] AS Array(Map(String, Tuple(arr Array(UInt8))))) AS a
FORMAT Null;

SELECT 'ok';

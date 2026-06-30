-- Tags: no-random-settings

DROP TABLE IF EXISTS nullable_array_type;

CREATE TABLE nullable_array_type_bad (a Nullable(Array(Int32))) ENGINE = Memory; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
CREATE TABLE nullable_array_type_bad_null_modifier (a Array(Int32) NULL) ENGINE = Memory; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
CREATE TABLE nullable_array_type_bad_default_null (a Array(Int32) DEFAULT NULL) ENGINE = Memory; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

CREATE TABLE ctas_nullable_array_bad ENGINE = Memory AS SELECT CAST([1, 2] AS Nullable(Array(Int32))) AS a; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
CREATE TABLE ctas_nullable_array_bad2 ENGINE = Memory AS SELECT CAST(NULL AS Nullable(Array(Int32))) AS a; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SET allow_experimental_nullable_array_type = 1;

CREATE TABLE nullable_array_type_null_modifier (a Array(Int32) NULL) ENGINE = Memory;
SELECT throwIf(toTypeName(a) != 'Nullable(Array(Int32))')
FROM nullable_array_type_null_modifier
FORMAT Null;
DROP TABLE nullable_array_type_null_modifier;

CREATE TABLE nullable_array_type_default_null (a Array(Int32) DEFAULT NULL) ENGINE = Memory;
SELECT throwIf(toTypeName(a) != 'Nullable(Array(Int32))')
FROM nullable_array_type_default_null
FORMAT Null;
DROP TABLE nullable_array_type_default_null;

SELECT throwIf(toTypeName(if(number % 2, [1, 2], CAST(NULL AS Nullable(Array(Int32))))) != 'Nullable(Array(Int32))')
FROM numbers(1)
FORMAT Null;

SELECT throwIf(toTypeName(multiIf(number % 2, [1, 2], CAST(NULL AS Nullable(Array(Int32))))) != 'Nullable(Array(Int32))')
FROM numbers(1)
FORMAT Null;

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

SELECT throwIf(NOT isNull(arraySlice(a, 1, 1)))
FROM nullable_array_type
WHERE id = 1
FORMAT Null;

SELECT throwIf(arraySlice(a, 2, 1) != [2])
FROM nullable_array_type
WHERE id = 3
FORMAT Null;

SELECT throwIf(arraySlice(CAST([1, 2, 3], 'Nullable(Array(UInt8))'), 2, 2) != [2, 3])
FORMAT Null;

DROP TABLE nullable_array_type;

SELECT arrayElementOrNull(a, 'k')
FROM system.one
ARRAY JOIN CAST([map('k', tuple([1, 2])), map()] AS Array(Map(String, Tuple(arr Array(UInt8))))) AS a
FORMAT Null;

SELECT 'ok';

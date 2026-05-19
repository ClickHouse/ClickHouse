-- Tags: no-random-settings

-- MergeTree roundtrip for Nullable(Array(Nullable(T))): outer and inner null maps must stay independent.

SET allow_experimental_nullable_array_type = 1;

DROP TABLE IF EXISTS nullable_array_nested;

CREATE TABLE nullable_array_nested
(
    id UInt8,
    a Nullable(Array(Nullable(Int32)))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO nullable_array_nested VALUES
    (1, NULL),
    (2, []),
    (3, [NULL, 10, NULL]);

SELECT throwIf(groupArray(isNull(a)) != [1, 0, 0])
FROM (SELECT a FROM nullable_array_nested ORDER BY id)
FORMAT Null;

SELECT throwIf(groupArray(arrayMap(x -> isNull(x), assumeNotNull(a))) != [[], [1, 0, 1]])
FROM (SELECT a FROM nullable_array_nested WHERE a IS NOT NULL ORDER BY id)
FORMAT Null;

DETACH TABLE nullable_array_nested;
ATTACH TABLE nullable_array_nested;

SELECT throwIf(groupArray(isNull(a)) != [1, 0, 0])
FROM (SELECT a FROM nullable_array_nested ORDER BY id)
FORMAT Null;

SELECT throwIf(groupArray(arrayMap(x -> isNull(x), assumeNotNull(a))) != [[], [1, 0, 1]])
FROM (SELECT a FROM nullable_array_nested WHERE a IS NOT NULL ORDER BY id)
FORMAT Null;

DROP TABLE nullable_array_nested;

SELECT 'ok';

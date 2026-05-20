-- Tags: no-random-settings

-- MergeTree roundtrip for Array(Nullable(Array(Nullable(T)))): outer and inner null maps
-- must remain independent at every array nesting level.

SET allow_experimental_nullable_array_type = 1;

DROP TABLE IF EXISTS nested_array_nullable_array;

CREATE TABLE nested_array_nullable_array
(
    id UInt8,
    a Array(Nullable(Array(Nullable(UInt8))))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO nested_array_nullable_array VALUES
    (1, [NULL]),
    (2, [[1, NULL, 3]]),
    (3, [NULL, []]);

SELECT throwIf(groupArray(arrayMap(x -> isNull(x), a)) != [[1], [0], [1, 0]])
FROM (SELECT a FROM nested_array_nullable_array ORDER BY id)
FORMAT Null;

SELECT throwIf(groupArray(arrayMap(
        x -> if(isNull(x), [], arrayMap(y -> isNull(y), assumeNotNull(x))),
        a)) != [[[]], [[0, 1, 0]], [[], []]])
FROM (SELECT a FROM nested_array_nullable_array ORDER BY id)
FORMAT Null;

DETACH TABLE nested_array_nullable_array;
ATTACH TABLE nested_array_nullable_array;

SELECT throwIf(groupArray(arrayMap(x -> isNull(x), a)) != [[1], [0], [1, 0]])
FROM (SELECT a FROM nested_array_nullable_array ORDER BY id)
FORMAT Null;

SELECT throwIf(groupArray(arrayMap(
        x -> if(isNull(x), [], arrayMap(y -> isNull(y), assumeNotNull(x))),
        a)) != [[[]], [[0, 1, 0]], [[], []]])
FROM (SELECT a FROM nested_array_nullable_array ORDER BY id)
FORMAT Null;

DROP TABLE nested_array_nullable_array;

SELECT 'ok';

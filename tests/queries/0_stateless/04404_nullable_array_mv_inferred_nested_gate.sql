-- Tags: no-random-settings

DROP TABLE IF EXISTS source_04404_nullable_array_mv;
DROP TABLE IF EXISTS mv_04404_nullable_array_mv;

SET allow_experimental_nullable_array_type = 1;

CREATE TABLE source_04404_nullable_array_mv
(
    a Nullable(Array(UInt8))
)
ENGINE = Memory;

SET allow_experimental_nullable_array_type = 0;

CREATE MATERIALIZED VIEW mv_04404_nullable_array_mv
ENGINE = Memory
AS SELECT tuple(a) AS t
FROM source_04404_nullable_array_mv; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET allow_experimental_nullable_array_type = 1;

DROP TABLE source_04404_nullable_array_mv;

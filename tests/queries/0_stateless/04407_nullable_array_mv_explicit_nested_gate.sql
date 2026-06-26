SET allow_experimental_nullable_array_type = 1;

CREATE TABLE src (a Nullable(Array(UInt8))) ENGINE = Memory;

SET allow_experimental_nullable_array_type = 0;

CREATE MATERIALIZED VIEW mv (t Tuple(a Nullable(Array(UInt8)))) ENGINE = Memory AS SELECT tuple(a) AS t FROM src; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE src;

DROP TABLE IF EXISTS t_tuple_codec_wrapped_nested;
DROP TABLE IF EXISTS t_tuple_codec_wrapped_json;
DROP TABLE IF EXISTS t_tuple_codec_add_wrapped;
DROP TABLE IF EXISTS t_tuple_codec_alter_nested;
DROP TABLE IF EXISTS t_tuple_codec_alter_json;
DROP TABLE IF EXISTS t_tuple_codec_direct_tuple;

SET enable_tuple_element_codecs = 1;

-- Nested stores child types in ASTNameTypePair nodes. An annotation below that
-- wrapper must be rejected instead of disappearing from the stored metadata.
CREATE TABLE t_tuple_codec_wrapped_nested
(
    value Nested(item Tuple(id UInt64 CODEC(Delta, LZ4), text String))
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
-- User visible message: Tuple element codec declarations through non-Tuple wrapper types are not supported

-- Typed JSON paths store child types in ASTObjectTypeArgument nodes and have
-- the same unsupported-wrapper boundary.
CREATE TABLE t_tuple_codec_wrapped_json
(
    value JSON(item Tuple(id UInt64 CODEC(Delta, LZ4), text String))
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- ADD COLUMN has its own ALTER parsing route and must reject both wrappers too.
CREATE TABLE t_tuple_codec_add_wrapped
(
    key UInt64
)
ENGINE = MergeTree
ORDER BY tuple();

ALTER TABLE t_tuple_codec_add_wrapped
    ADD COLUMN nested_value Nested(item Tuple(id UInt64 CODEC(Delta, LZ4), text String)); -- { serverError BAD_ARGUMENTS }

ALTER TABLE t_tuple_codec_add_wrapped
    ADD COLUMN json_value JSON(item Tuple(id UInt64 CODEC(Delta, LZ4), text String)); -- { serverError BAD_ARGUMENTS }

-- Typed MODIFY COLUMN uses the same explicit rejection boundary.
CREATE TABLE t_tuple_codec_alter_nested
(
    value Nested(item Tuple(id UInt64, text String))
)
ENGINE = MergeTree
ORDER BY tuple();

ALTER TABLE t_tuple_codec_alter_nested
    MODIFY COLUMN value Nested(item Tuple(id UInt64 CODEC(Delta, LZ4), text String)); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_tuple_codec_alter_json
(
    value JSON(item Tuple(id UInt64, text String))
)
ENGINE = MergeTree
ORDER BY tuple();

ALTER TABLE t_tuple_codec_alter_json
    MODIFY COLUMN value JSON(item Tuple(id UInt64 CODEC(Delta, LZ4), text String)); -- { serverError BAD_ARGUMENTS }

-- A direct Tuple chain rooted at the owning column remains supported.
CREATE TABLE t_tuple_codec_direct_tuple
(
    value Tuple(nested Tuple(id UInt64 CODEC(Delta, LZ4), text String))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT
    position(compression_codec, 'id UInt64 CODEC(Delta(8), LZ4)') > 0,
    position(type, 'CODEC') = 0
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_direct_tuple' AND name = 'value';

DROP TABLE t_tuple_codec_alter_nested;
DROP TABLE t_tuple_codec_alter_json;
DROP TABLE t_tuple_codec_add_wrapped;
DROP TABLE t_tuple_codec_direct_tuple;

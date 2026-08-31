DROP TABLE IF EXISTS t_tuple_codec_as_select;
DROP TABLE IF EXISTS t_tuple_codec_wrapped_array;
DROP TABLE IF EXISTS t_tuple_codec_wrapped_map;
DROP TABLE IF EXISTS t_tuple_codec_wrapper_controls;
DROP TABLE IF EXISTS t_tuple_codec_remove_create;
DROP TABLE IF EXISTS t_tuple_codec_remove_add;
DROP TABLE IF EXISTS t_tuple_codec_no_declaration;
DROP TABLE IF EXISTS t_tuple_codec_quantized;
DROP TABLE IF EXISTS t_tuple_codec_alias;
DROP TABLE IF EXISTS t_tuple_codec_log;
DROP TABLE IF EXISTS t_tuple_codec_nested_control;

-- General data-type parsers never attach storage metadata to expression-level tuples.
SELECT CAST((1, 'x') AS Tuple(id UInt64 CODEC(Delta, ZSTD), value String)); -- { serverError SYNTAX_ERROR }
SELECT CAST((1, 'x'), 'Tuple(id UInt64 CODEC(Delta, ZSTD), value String)'); -- { serverError SYNTAX_ERROR }
SELECT toTypeName(CAST((1, 'x') AS Tuple(id UInt64, value String)));
SELECT toTypeName(tuple(toUInt64(1), 'x'));

CREATE TABLE t_tuple_codec_as_select
ENGINE = MergeTree
ORDER BY tuple()
AS SELECT tuple(toUInt64(1), 'x') AS value;

SELECT position(create_table_query, 'CODEC') = 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_tuple_codec_as_select';
DROP TABLE t_tuple_codec_as_select;

-- An annotation below a non-`Tuple` wrapper is rejected instead of being ignored.
CREATE TABLE t_tuple_codec_wrapped_array
(
    value Array(Tuple(id UInt64 CODEC(Delta, ZSTD), text String))
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_tuple_codec_wrapped_map
(
    value Map(String, Tuple(id UInt64 CODEC(Delta, ZSTD), text String))
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_tuple_codec_wrapper_controls
(
    array_value Array(Tuple(id UInt64, text String)) CODEC(ZSTD(1)),
    map_value Map(String, Tuple(id UInt64, text String)) CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count(), countIf(compression_codec = 'CODEC(ZSTD(1))')
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_wrapper_controls';
DROP TABLE t_tuple_codec_wrapper_controls;

-- Element-level `REMOVE CODEC` is ALTER-only.
CREATE TABLE t_tuple_codec_remove_create
(
    value Tuple(id UInt64 REMOVE CODEC, text String)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError SYNTAX_ERROR }

CREATE TABLE t_tuple_codec_remove_add (key UInt64) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_tuple_codec_remove_add
    ADD COLUMN value Tuple(id UInt64 REMOVE CODEC, text String); -- { serverError SYNTAX_ERROR }
DROP TABLE t_tuple_codec_remove_add;

CREATE TABLE t_tuple_codec_no_declaration
(
    value Tuple(id UInt64, text String) CODEC(LZ4)
)
ENGINE = MergeTree
ORDER BY tuple();

ALTER TABLE t_tuple_codec_no_declaration
    MODIFY COLUMN value Tuple(id UInt64 REMOVE CODEC, text String); -- { serverError BAD_ARGUMENTS }

ALTER TABLE t_tuple_codec_no_declaration
    MODIFY COLUMN value Tuple(
        id UInt64,
        wrapped Array(Tuple(number UInt64 CODEC(Delta, ZSTD), text String))
    ); -- { serverError BAD_ARGUMENTS }

ALTER TABLE t_tuple_codec_no_declaration
    MODIFY COLUMN value Tuple(
        id UInt64,
        wrapped Map(String, Tuple(number UInt64 CODEC(Delta, ZSTD), text String))
    ); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_tuple_codec_no_declaration;

-- Codec-driven custom serialization is not path-aware yet.
CREATE TABLE t_tuple_codec_quantized
(
    value Tuple(vector Array(Float32) CODEC(Quantized('rabitq', 8)), text String)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError NOT_IMPLEMENTED }

-- Codec policies cannot be attached to an `ALIAS` column.
CREATE TABLE t_tuple_codec_alias
(
    source UInt64,
    value Tuple(id UInt64 CODEC(LZ4), text String) ALIAS tuple(source, toString(source))
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- `Log` rejects a per-element policy when its sink is constructed.
CREATE TABLE t_tuple_codec_log
(
    value Tuple(id UInt64 CODEC(LZ4), text String CODEC(ZSTD(1)))
)
ENGINE = Log;
INSERT INTO t_tuple_codec_log VALUES ((1, 'x')); -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_tuple_codec_log;

-- A direct nested-`Tuple` chain is the positive boundary control.
CREATE TABLE t_tuple_codec_nested_control
(
    value Tuple(nested Tuple(id UInt64 CODEC(Delta, LZ4), text String))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT
    position(compression_codec, 'id UInt64 CODEC(Delta(8), LZ4)') > 0,
    position(type, 'CODEC') = 0
FROM system.columns
WHERE database = currentDatabase() AND table = 't_tuple_codec_nested_control' AND name = 'value';

DROP TABLE t_tuple_codec_nested_control;

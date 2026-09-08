-- Dropping a whole Nested group must not remove a column the table engine still points at,
-- and must not bypass materialized view protection on the other ALTER-capable engines.

DROP TABLE IF EXISTS nested_drop_sign_column;

CREATE TABLE nested_drop_sign_column
(
    `n.s` Int8,
    `n.b` UInt64,
    x UInt64
)
ENGINE = CollapsingMergeTree(`n.s`)
ORDER BY x;

ALTER TABLE nested_drop_sign_column DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE nested_drop_sign_column CLEAR COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE nested_drop_sign_column;

DROP TABLE IF EXISTS nested_drop_version_column;

CREATE TABLE nested_drop_version_column
(
    `n.version` UInt64,
    `n.b` UInt64,
    x UInt64
)
ENGINE = ReplacingMergeTree(`n.version`)
ORDER BY x;

ALTER TABLE nested_drop_version_column DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE nested_drop_version_column;

DROP TABLE IF EXISTS nested_drop_null_source;

CREATE TABLE nested_drop_null_source
(
    `n.a` UInt64,
    `n.b` UInt64,
    x UInt64
)
ENGINE = Null;

CREATE MATERIALIZED VIEW nested_drop_null_mv
ENGINE = Null
AS SELECT `n.a` FROM nested_drop_null_source;

ALTER TABLE nested_drop_null_source DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP VIEW nested_drop_null_mv;
DROP TABLE nested_drop_null_source;

DROP TABLE IF EXISTS nested_drop_buffer_dest;
DROP TABLE IF EXISTS nested_drop_buffer;

CREATE TABLE nested_drop_buffer_dest
(
    `n.a` UInt64,
    `n.b` UInt64,
    x UInt64
)
ENGINE = MergeTree
ORDER BY x;

CREATE TABLE nested_drop_buffer AS nested_drop_buffer_dest
ENGINE = Buffer(currentDatabase(), nested_drop_buffer_dest, 1, 1, 1, 1, 1, 1, 1);

CREATE MATERIALIZED VIEW nested_drop_buffer_mv
ENGINE = Null
AS SELECT `n.a` FROM nested_drop_buffer;

ALTER TABLE nested_drop_buffer DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP VIEW nested_drop_buffer_mv;
DROP TABLE nested_drop_buffer;
DROP TABLE nested_drop_buffer_dest;

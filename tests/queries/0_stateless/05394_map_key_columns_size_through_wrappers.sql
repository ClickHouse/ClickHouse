-- `length`/`empty` of a `with_key_columns` Map must not be rewritten to the `size0`
-- subcolumn, which such a Map does not store, also when read through a wrapper storage.

SET allow_experimental_map_key_columns = 1;

SET allow_experimental_alias_table_engine = 1;

DROP TABLE IF EXISTS t_wrap_mv;
DROP TABLE IF EXISTS t_wrap_alias;
DROP TABLE IF EXISTS t_wrap_buffer;
DROP TABLE IF EXISTS t_wrap_merge;
DROP VIEW IF EXISTS t_wrap_view;
DROP TABLE IF EXISTS t_wrap;
DROP TABLE IF EXISTS t_wrap_mv_target;

CREATE TABLE t_wrap (id UInt8, m Map(String, UInt8)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', serialization_info_version = 'with_types', min_bytes_for_wide_part = 0;
CREATE TABLE t_wrap_mv_target AS t_wrap;
CREATE MATERIALIZED VIEW t_wrap_mv TO t_wrap_mv_target AS SELECT * FROM t_wrap;

INSERT INTO t_wrap VALUES (1, {'a':1,'b':2}), (2, {});

CREATE VIEW t_wrap_view AS SELECT * FROM t_wrap;
CREATE TABLE t_wrap_merge AS t_wrap ENGINE = Merge(currentDatabase(), '^t_wrap$');
CREATE TABLE t_wrap_buffer AS t_wrap ENGINE = Buffer(currentDatabase(), t_wrap, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);
CREATE TABLE t_wrap_alias ENGINE = Alias(t_wrap);

SELECT 'table', id, length(m), empty(m), notEmpty(m) FROM t_wrap ORDER BY id;
SELECT 'view', id, length(m), empty(m), notEmpty(m) FROM t_wrap_view ORDER BY id;
SELECT 'merge', id, length(m), empty(m), notEmpty(m) FROM t_wrap_merge ORDER BY id;
SELECT 'buffer', id, length(m), empty(m), notEmpty(m) FROM t_wrap_buffer ORDER BY id;
SELECT 'alias', id, length(m), empty(m), notEmpty(m) FROM t_wrap_alias ORDER BY id;
SELECT 'mv', id, length(m), empty(m), notEmpty(m) FROM t_wrap_mv ORDER BY id;

DROP TABLE t_wrap_mv;
DROP TABLE t_wrap_alias;
DROP TABLE t_wrap_buffer;
DROP TABLE t_wrap_merge;
DROP VIEW t_wrap_view;
DROP TABLE t_wrap;
DROP TABLE t_wrap_mv_target;

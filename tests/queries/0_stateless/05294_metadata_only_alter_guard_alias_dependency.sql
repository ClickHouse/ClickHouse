-- A stored MATERIALIZED column that reads the altered named tuple only through an ALIAS column
-- must block a metadata-only ALTER exactly like a direct reference does: nothing would recompute it.

DROP TABLE IF EXISTS t_guard_plain_alias;
DROP TABLE IF EXISTS t_guard_function_alias;

CREATE TABLE t_guard_plain_alias (t Tuple(a Int64), t_alias ALIAS t, m String MATERIALIZED toString(t_alias))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_guard_plain_alias (t) VALUES ((1));

CREATE TABLE t_guard_function_alias (t Tuple(a Int64), s ALIAS toString(t), m String MATERIALIZED s)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_guard_function_alias (t) VALUES ((1));

ALTER TABLE t_guard_plain_alias MODIFY COLUMN t Tuple(a Int64, b String)
SETTINGS allow_metadata_only_named_tuple_alter = 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE t_guard_function_alias MODIFY COLUMN t Tuple(a Int64, b String)
SETTINGS allow_metadata_only_named_tuple_alter = 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Without the lazy conversion the change runs as a full mutation that recomputes the column.
ALTER TABLE t_guard_function_alias MODIFY COLUMN t Tuple(a Int64, b String)
SETTINGS allow_metadata_only_named_tuple_alter = 0, mutations_sync = 2;

SELECT t, m FROM t_guard_function_alias;

DROP TABLE t_guard_plain_alias;
DROP TABLE t_guard_function_alias;

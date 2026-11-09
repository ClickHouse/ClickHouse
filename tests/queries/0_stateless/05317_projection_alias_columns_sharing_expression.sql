-- Two columns of a normal projection which share an expression and differ only by their aliases.
-- The projection metadata ignores aliases, so it stores them as a single column, and the calculation
-- of the projection on insert and on merge must give exactly that column.

DROP TABLE IF EXISTS t_projection_alias_columns;

CREATE TABLE t_projection_alias_columns (a UInt64, b String)
ENGINE = MergeTree ORDER BY b
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_alias_columns ADD PROJECTION p (SELECT a AS x, a AS y ORDER BY x);

INSERT INTO t_projection_alias_columns SELECT number, toString(number) FROM numbers(10);
INSERT INTO t_projection_alias_columns SELECT number, toString(number) FROM numbers(5);

SELECT arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_alias_columns' AND name = 'p' AND active;

OPTIMIZE TABLE t_projection_alias_columns FINAL;

SELECT arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_alias_columns' AND name = 'p' AND active;

SELECT count(), sum(a) FROM t_projection_alias_columns;

DROP TABLE t_projection_alias_columns;

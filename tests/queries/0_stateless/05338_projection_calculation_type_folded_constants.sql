-- A normal projection stores the columns its query requires. The Analyzer folds `isNull` of a column
-- which is not `Nullable` into a constant, which must not make the projection lose that column.

DROP TABLE IF EXISTS t_projection_folded_constants;

CREATE TABLE t_projection_folded_constants (a UInt64, b String, c Nullable(String))
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4, materialize_projections_on_insert = 1;

ALTER TABLE t_projection_folded_constants ADD PROJECTION p INDEX * APPLY isNull TYPE basic();

INSERT INTO t_projection_folded_constants SELECT number, toString(number % 5), toString(number) FROM numbers(20);

SELECT name, arraySort(groupUniqArray(column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_folded_constants' AND active
GROUP BY name ORDER BY name;

SELECT count(), sum(a) FROM t_projection_folded_constants;

ALTER TABLE t_projection_folded_constants MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

SELECT count(), sum(a) FROM t_projection_folded_constants;

DROP TABLE t_projection_folded_constants;

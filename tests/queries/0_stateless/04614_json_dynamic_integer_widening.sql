-- Tags: no-fasttest

SET enable_json_type = 1;

DROP TABLE IF EXISTS t_json_dynamic_integer_widening;
CREATE TABLE t_json_dynamic_integer_widening (j Array(JSON)) ENGINE = Memory;

-- A small integer inserted first must not pin the JSON dynamic path to a narrow
-- integer type and then wrap a larger later value (Int8: 1944 -> -104). The
-- INSERT ... VALUES(CAST(...)) path builds the column via ColumnObject::insert ->
-- ColumnDynamic::insert -> ColumnVector::tryInsert, which must widen the variant
-- instead of silently truncating.
INSERT INTO t_json_dynamic_integer_widening (j)
VALUES (CAST([map('duration_ms', 50), map('duration_ms', 1944)] AS Array(JSON)));

SELECT arrayMap(x -> toInt64(x.duration_ms), j) FROM t_json_dynamic_integer_widening;

DROP TABLE t_json_dynamic_integer_widening;

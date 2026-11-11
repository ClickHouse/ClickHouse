-- Regression test: a TTL `WHERE` referencing a subcolumn (e.g. of a `JSON` column) used to
-- throw a logical error `original.has_value()` at CREATE, because the TTL analysis reported
-- the subcolumn in its required source columns and the mapping back to the table columns
-- asserted that every reported name is a top-level column.

DROP TABLE IF EXISTS t_ttl_where_json_subcolumn;

CREATE TABLE t_ttl_where_json_subcolumn
(
    id UInt32,
    d DateTime,
    j JSON(ts UInt32)
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY WHERE j.ts > 0
SETTINGS min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0;

INSERT INTO t_ttl_where_json_subcolumn VALUES (1, now() - INTERVAL 10 DAY, '{"ts" : 1}'), (2, now() - INTERVAL 10 DAY, '{"ts" : 0}'), (3, now(), '{"ts" : 1}');

OPTIMIZE TABLE t_ttl_where_json_subcolumn FINAL;

-- Row 1 is expired and matches the WHERE on the subcolumn; rows 2 and 3 must survive.
SELECT id FROM t_ttl_where_json_subcolumn ORDER BY id;

-- Rebuilding the TTL expression from the stored source columns (which include the subcolumn)
-- must also work, e.g. for ALTER ... MODIFY TTL and the subsequent materialization.
ALTER TABLE t_ttl_where_json_subcolumn MODIFY TTL d + INTERVAL 2 DAY WHERE j.ts > 42 SETTINGS mutations_sync = 2;

SELECT id FROM t_ttl_where_json_subcolumn ORDER BY id;

DROP TABLE t_ttl_where_json_subcolumn;

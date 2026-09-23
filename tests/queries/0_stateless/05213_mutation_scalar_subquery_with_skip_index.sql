-- A skip index over the updated column makes the mutation analyse its stages a second time, without
-- executing anything, to learn the header the index is rebuilt from. In that analysis every scalar
-- subquery is a placeholder, and the placeholder of a `Nullable` result was `NULL`: the enclosing
-- conversion to the non-Nullable column type is evaluated while the header is computed, so the
-- mutation failed with `Cannot convert NULL value to non-Nullable type` before the value was ever
-- read. The mutation runs in the server's own context, so the case is reached when the old analyzer
-- is the server default; with the new one the placeholder is built differently and the test only
-- pins the result.

DROP TABLE IF EXISTS t_upd_src;
DROP TABLE IF EXISTS t_upd_idx;

CREATE TABLE t_upd_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_upd_src VALUES (2);

CREATE TABLE t_upd_idx (id UInt64, v UInt64, INDEX iv v TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_upd_idx VALUES (1, 0), (2, 0), (3, 0);

-- A scalar subquery whose result is `Nullable` (an aggregate over a possibly empty set).
ALTER TABLE t_upd_idx UPDATE v = (SELECT max(id) FROM t_upd_src) WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT v FROM t_upd_idx WHERE id = 1;

-- A scalar nested into another scalar, as a common table expression builds it.
ALTER TABLE t_upd_idx UPDATE v = (WITH s AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM t_upd_src SETTINGS enable_global_with_statement = 0)) WHERE id = 2 SETTINGS mutations_sync = 2;
SELECT v FROM t_upd_idx WHERE id = 2;

DROP TABLE t_upd_idx;
DROP TABLE t_upd_src;

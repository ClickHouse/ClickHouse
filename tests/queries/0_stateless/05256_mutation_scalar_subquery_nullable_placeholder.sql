-- A mutation is analysed without being executed before it runs: when the `ALTER` is validated, and once
-- more when a skip index over the updated column needs the header the index is rebuilt from. In that
-- analysis every scalar subquery is a placeholder, and the placeholder of a `Nullable` result was `NULL`:
-- the enclosing conversion to the non-Nullable column type is evaluated while the header is computed, so
-- the mutation failed with `Cannot convert NULL value to non-Nullable type` before the value was ever
-- read. The mutation runs in the server's own context, so this covers the analyzer the server runs by
-- default; the old-analyzer path is covered by 05255_mutation_scalar_subquery_with_skip_index.

DROP TABLE IF EXISTS t_upd_src;
DROP TABLE IF EXISTS t_upd_dst;
DROP TABLE IF EXISTS t_upd_plain;
DROP TABLE IF EXISTS t_upd_null;

CREATE TABLE t_upd_src (id UInt64, n Nullable(UInt64), lc LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_upd_src VALUES (2, 5, 'x');

CREATE TABLE t_upd_dst (id UInt64, v UInt64, s String, INDEX iv v TYPE minmax GRANULARITY 1, INDEX is s TYPE set(1) GRANULARITY 1) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_upd_dst VALUES (1, 0, ''), (2, 0, ''), (3, 0, ''), (4, 0, '');

-- The result of the scalar is `Nullable`: an aggregate over a `Nullable` column.
ALTER TABLE t_upd_dst UPDATE v = (SELECT max(n) FROM t_upd_src) WHERE id = 1 SETTINGS mutations_sync = 2;
-- A `Nullable` column itself.
ALTER TABLE t_upd_dst UPDATE v = (SELECT n FROM t_upd_src) WHERE id = 2 SETTINGS mutations_sync = 2;
-- `LowCardinality(Nullable(...))`.
ALTER TABLE t_upd_dst UPDATE s = (SELECT lc FROM t_upd_src) WHERE id = 3 SETTINGS mutations_sync = 2;
-- A non-Nullable result is wrapped into `Nullable` as the subquery may return no rows.
ALTER TABLE t_upd_dst UPDATE v = (SELECT id FROM t_upd_src) WHERE id = 4 SETTINGS mutations_sync = 2;

SELECT * FROM t_upd_dst ORDER BY id;

-- A table without any skip index: the analysis also happens when the `ALTER` is validated.
CREATE TABLE t_upd_plain (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_upd_plain VALUES (1, 0);
ALTER TABLE t_upd_plain UPDATE v = (SELECT max(n) FROM t_upd_src) WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT * FROM t_upd_plain;

-- `Nothing` has no value other than `NULL`: the placeholder stays `NULL`, and a `Nullable` column takes it.
CREATE TABLE t_upd_null (id UInt64, v Nullable(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_upd_null VALUES (1, 0);
ALTER TABLE t_upd_null UPDATE v = (SELECT NULL) WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT * FROM t_upd_null;

DROP TABLE t_upd_null;
DROP TABLE t_upd_plain;
DROP TABLE t_upd_dst;
DROP TABLE t_upd_src;

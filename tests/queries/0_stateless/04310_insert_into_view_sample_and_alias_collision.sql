-- Follow-up review tests for INSERT into regular views (issue #91535).
--
-- 1. A view whose query contains `SAMPLE` is not a simple pass-through projection:
--    the read path samples rows, but the insert path forwards every row. It must be
--    rejected at INSERT time like the other read-only clauses.
--
-- 2. When a column alias collides with a target column name, the view-to-target rename
--    must still route every value to the correct target column. For
--    `CREATE VIEW v AS SELECT a AS b, b AS a FROM t` the mapping holds both `b -> a`
--    and `a -> b`; a name-based in-place rename could pick an already-renamed column.
--
-- 3. A `WHERE` constraint violated by a row in the middle of a batch must reject the
--    whole INSERT and tear down the inner insert pipeline cleanly (no partial write,
--    no hanging executor).

DROP TABLE IF EXISTS t_sample;
DROP TABLE IF EXISTS t_swap;
DROP TABLE IF EXISTS t_where;
DROP VIEW IF EXISTS v_sample;
DROP VIEW IF EXISTS v_swap;
DROP VIEW IF EXISTS v_where;

-- 1. SAMPLE is rejected.
CREATE TABLE t_sample (a UInt64, b String) ENGINE = MergeTree ORDER BY a SAMPLE BY a;
CREATE VIEW v_sample AS SELECT a, b FROM t_sample SAMPLE 0.1;
INSERT INTO v_sample VALUES (1, 'x'); -- { serverError NOT_IMPLEMENTED }

-- 2. Colliding alias swap routes values to the correct target columns.
--    view.b -> t.a and view.a -> t.b, so (view.b = 10, view.a = 20) must store (t.a = 10, t.b = 20).
CREATE TABLE t_swap (a Int32, b Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE VIEW v_swap AS SELECT a AS b, b AS a FROM t_swap;
INSERT INTO v_swap (b, a) VALUES (10, 20);
SELECT 'swap:', a, b FROM t_swap;

-- 3. A constraint violation in the middle of a batch rejects the whole INSERT,
--    leaving no rows behind.
CREATE TABLE t_where (a Int32, b String) ENGINE = MergeTree ORDER BY a;
CREATE VIEW v_where AS SELECT a, b FROM t_where WHERE a > 0;
-- Pin async_insert=0 so the two-row batch is processed atomically and the "no partial write"
-- assertion is deterministic regardless of the server's async-insert default.
INSERT INTO v_where SETTINGS async_insert = 0 VALUES (1, 'ok'), (-5, 'bad'); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'where_rows:', count() FROM t_where;

DROP VIEW v_where;
DROP VIEW v_swap;
DROP VIEW v_sample;
DROP TABLE t_where;
DROP TABLE t_swap;
DROP TABLE t_sample;

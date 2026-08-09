-- Follow-up review tests for INSERT into regular views (issue #91535).
--
-- 1. Views with `ARRAY JOIN`, `QUALIFY`, or `WINDOW` are not simple projections.
--    They must be rejected at INSERT time the same way as `JOIN`/`GROUP BY`/`HAVING`/`LIMIT`.
--
-- 2. A partial `INSERT INTO view (subset)` where the omitted view column is not at the
--    tail of the column list must still route values to the correct target columns.
--    The inner `INSERT` pipeline forwards columns by position; if `materializeTargetDefaults`
--    erases the omitted column and reinserts at the end, the column order changes and
--    values are written to the wrong target column.

DROP TABLE IF EXISTS t_target;
DROP TABLE IF EXISTS t_arr;
DROP VIEW IF EXISTS v_array_join;
DROP VIEW IF EXISTS v_qualify;
DROP VIEW IF EXISTS v_window;
DROP VIEW IF EXISTS v_star_ordering;

CREATE TABLE t_target (a UInt8, b UInt8 DEFAULT 7, c UInt8) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_arr (a Int32, arr Array(Int32)) ENGINE = MergeTree ORDER BY a;

-- 1a. ARRAY JOIN: not a simple projection.
CREATE VIEW v_array_join AS SELECT a FROM t_arr ARRAY JOIN arr AS x;
INSERT INTO v_array_join VALUES (1); -- { serverError NOT_IMPLEMENTED }

-- 1b. QUALIFY: filter applied after a window function; ignored on the write path.
CREATE VIEW v_qualify AS SELECT a FROM t_target QUALIFY row_number() OVER (ORDER BY a) = 1;
INSERT INTO v_qualify VALUES (1); -- { serverError NOT_IMPLEMENTED }

-- 1c. WINDOW clause: named windows are not simple projections.
CREATE VIEW v_window AS SELECT a FROM t_target WINDOW w AS (ORDER BY a);
INSERT INTO v_window VALUES (1); -- { serverError NOT_IMPLEMENTED }

-- 2. Partial INSERT must preserve column order in the inner pipeline.
--    `b` is the *middle* column. With `INSERT INTO v (a, c)`, `b` is omitted and filled with
--    its target DEFAULT (7). The stored row must have `(a, b, c) = (1, 7, 9)`, not (1, 9, 7).
CREATE VIEW v_star_ordering AS SELECT * FROM t_target;
INSERT INTO v_star_ordering (a, c) VALUES (1, 9);
SELECT 'middle_default:', a, b, c FROM t_target ORDER BY a;

DROP VIEW v_star_ordering;
DROP VIEW v_window;
DROP VIEW v_qualify;
DROP VIEW v_array_join;
DROP TABLE t_arr;
DROP TABLE t_target;

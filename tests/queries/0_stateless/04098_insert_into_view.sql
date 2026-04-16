-- Test: INSERT into regular views (issue #91535)

DROP TABLE IF EXISTS t_target;
DROP VIEW IF EXISTS v_simple;
DROP VIEW IF EXISTS v_subset;
DROP VIEW IF EXISTS v_alias;
DROP VIEW IF EXISTS v_where;
DROP VIEW IF EXISTS v_order_by;
DROP VIEW IF EXISTS v_limit;
DROP VIEW IF EXISTS v_group_by;
DROP VIEW IF EXISTS v_distinct;
DROP VIEW IF EXISTS v_join;

CREATE TABLE t_target (a Int32, b String, c Float64 DEFAULT 0.5) ENGINE = MergeTree ORDER BY a;

-- 1. Basic INSERT into a simple view (SELECT * FROM table)
CREATE VIEW v_simple AS SELECT * FROM t_target;
INSERT INTO v_simple VALUES (1, 'hello', 1.0);
SELECT 'simple:', a, b, c FROM t_target ORDER BY a;

-- 2. INSERT into a view that selects a subset of columns (missing columns get defaults)
CREATE VIEW v_subset AS SELECT a, b FROM t_target;
INSERT INTO v_subset VALUES (2, 'world');
SELECT 'subset:', a, b, c FROM t_target ORDER BY a;

-- 3. INSERT into a view with column aliases
CREATE VIEW v_alias AS SELECT a AS id, b AS name FROM t_target;
INSERT INTO v_alias VALUES (3, 'alias_test');
SELECT 'alias:', a, b, c FROM t_target ORDER BY a;

-- 4. INSERT into a view with WHERE (acts as constraint, should pass)
CREATE VIEW v_where AS SELECT a, b FROM t_target WHERE a > 0;
INSERT INTO v_where VALUES (4, 'positive');
SELECT 'where_pass:', a, b FROM t_target WHERE a = 4;

-- 5. INSERT into a view with WHERE (should fail: constraint violated)
INSERT INTO v_where VALUES (-1, 'negative'); -- { serverError VIOLATED_CONSTRAINT }

-- 6. INSERT into a view with ORDER BY (allowed, ORDER BY is ignored for inserts)
CREATE VIEW v_order_by AS SELECT a, b FROM t_target ORDER BY a DESC;
INSERT INTO v_order_by VALUES (5, 'ordered');
SELECT 'order_by:', a, b FROM t_target WHERE a = 5;

-- 7. INSERT into a view with LIMIT (should fail)
CREATE VIEW v_limit AS SELECT a, b FROM t_target LIMIT 10;
INSERT INTO v_limit VALUES (6, 'limited'); -- { serverError NOT_IMPLEMENTED }

-- 8. INSERT into a view with GROUP BY (should fail)
CREATE VIEW v_group_by AS SELECT a, count() AS cnt FROM t_target GROUP BY a;
INSERT INTO v_group_by VALUES (7, 1); -- { serverError NOT_IMPLEMENTED }

-- 9. INSERT into a view with DISTINCT (should fail)
CREATE VIEW v_distinct AS SELECT DISTINCT a, b FROM t_target;
INSERT INTO v_distinct VALUES (8, 'distinct'); -- { serverError NOT_IMPLEMENTED }

-- 10. INSERT into a view with JOIN (should fail)
CREATE VIEW v_join AS SELECT t1.a, t1.b FROM t_target AS t1 JOIN t_target AS t2 ON t1.a = t2.a;
INSERT INTO v_join VALUES (9, 'joined'); -- { serverError NOT_IMPLEMENTED }

-- 11. Multiple inserts into same view
TRUNCATE TABLE t_target;
INSERT INTO v_simple VALUES (10, 'first', 1.1), (20, 'second', 2.2), (30, 'third', 3.3);
SELECT 'multi:', a, b, c FROM t_target ORDER BY a;

-- 12. INSERT ... SELECT into view
INSERT INTO v_subset SELECT number, toString(number) FROM numbers(3);
SELECT 'insert_select:', a, b FROM t_target WHERE c = 0.5 ORDER BY a;

-- Cleanup
DROP VIEW v_join;
DROP VIEW v_distinct;
DROP VIEW v_group_by;
DROP VIEW v_limit;
DROP VIEW v_order_by;
DROP VIEW v_where;
DROP VIEW v_alias;
DROP VIEW v_subset;
DROP VIEW v_simple;
DROP TABLE t_target;

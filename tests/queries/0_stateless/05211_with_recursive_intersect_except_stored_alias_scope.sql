-- Tags: no-old-analyzer
-- no-old-analyzer: `WITH RECURSIVE` needs the analyzer.

-- A recursive element whose body is an `INTERSECT` / `EXCEPT` is rejected by the analyzer as an
-- unsupported union mode, and that check fires only while the element still references itself.
-- When an enclosing `SELECT` declares a same-named `WITH` element, `ApplyWithSubqueryVisitor`
-- must not expand it into the recursive members, or the self-reference disappears, the stored
-- query is accepted although the identical live query is rejected, and every read of the view
-- runs the expanded query instead of raising. `ApplyWithSubqueryVisitor` therefore has to
-- classify the body like `QueryTreeBuilder`: the operands of an `INTERSECT` / `EXCEPT`, reached
-- through any number of single-branch wrappers, are branches of a recursive element too.

DROP TABLE IF EXISTS v_intersect;
DROP TABLE IF EXISTS v_except;
DROP TABLE IF EXISTS v_wrapped;
DROP TABLE IF EXISTS v_union;

-- Control: the direct query is rejected.
WITH src AS (SELECT 111 AS id)
SELECT id FROM (
    WITH RECURSIVE src AS (SELECT 1 AS id INTERSECT SELECT id FROM src)
    SELECT id FROM src); -- { serverError UNSUPPORTED_METHOD }

-- The stored query must be rejected the same way, not accepted with `src` bound to the enclosing element.
CREATE VIEW v_intersect AS
WITH src AS (SELECT 111 AS id)
SELECT id FROM (
    WITH RECURSIVE src AS (SELECT 1 AS id INTERSECT SELECT id FROM src)
    SELECT id FROM src); -- { serverError UNSUPPORTED_METHOD }

CREATE VIEW v_except AS
WITH src AS (SELECT 111 AS id)
SELECT id FROM (
    WITH RECURSIVE src AS (SELECT 1 AS id EXCEPT SELECT id FROM src)
    SELECT id FROM src); -- { serverError UNSUPPORTED_METHOD }

-- Single-branch wrappers around the body do not change what it is.
CREATE VIEW v_wrapped AS
WITH src AS (SELECT 111 AS id)
SELECT id FROM (
    WITH RECURSIVE src AS ((SELECT 1 AS id INTERSECT SELECT id FROM src))
    SELECT id FROM src); -- { serverError UNSUPPORTED_METHOD }

SELECT 'views created', count() FROM system.tables WHERE database = currentDatabase() AND name IN ('v_intersect', 'v_except', 'v_wrapped');

-- Control: the `UNION ALL` body keeps its self-reference and recurses, with the enclosing element
-- still visible in the seed.
CREATE VIEW v_union AS
WITH src AS (SELECT 1 AS id)
SELECT id FROM (
    WITH RECURSIVE src AS (SELECT id FROM src UNION ALL SELECT id + 1 FROM src WHERE id < 3)
    SELECT id FROM src);
SELECT 'union', id FROM v_union ORDER BY id;

-- Control: with no enclosing element to shadow, an `INTERSECT` body inside a mutation is rejected too.
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 0);
ALTER TABLE t UPDATE v = (
    WITH src AS (SELECT 111 AS id)
    SELECT id FROM (
        WITH RECURSIVE src AS (SELECT 1 AS id INTERSECT SELECT id FROM src)
        SELECT id FROM src)) WHERE 1 SETTINGS mutations_sync = 2; -- { serverError UNSUPPORTED_METHOD }
SELECT 'mutation untouched', v FROM t;

DROP TABLE t;
DROP TABLE v_union;

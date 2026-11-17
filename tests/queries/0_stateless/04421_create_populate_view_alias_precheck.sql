-- Regression test for https://github.com/ClickHouse/ClickHouse/pull/108048
-- A POPULATE materialized view whose ORDER BY references a column alias from the view's column list must
-- populate correctly: the view's column alias list is applied to the SELECT before it is planned, so
-- `ORDER BY x` (where `(x)` renames the SELECT column `y`) resolves instead of failing with
-- UNKNOWN_IDENTIFIER.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS mv_04421;
DROP TABLE IF EXISTS src_04421;

CREATE TABLE src_04421 (y Int) ENGINE = Memory;
INSERT INTO src_04421 VALUES (3), (1), (2);

-- `(x)` renames the single SELECT column `y` to `x`; `ORDER BY x` is only resolvable once that alias is applied.
CREATE MATERIALIZED VIEW mv_04421 (x) ENGINE = Memory POPULATE AS SELECT y FROM src_04421 ORDER BY x;

SELECT x FROM mv_04421 ORDER BY x;

DROP TABLE mv_04421;
DROP TABLE src_04421;

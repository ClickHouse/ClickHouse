-- Regression test for https://github.com/ClickHouse/ClickHouse/pull/108048
-- The up-front access pre-check for queries that populate a table immediately (CREATE ... AS SELECT, or a
-- materialized/window view with POPULATE) builds the full query plan of the SELECT before the table is
-- created. It must run after the view's column alias list has already been applied to the SELECT; otherwise
-- a POPULATE materialized view whose ORDER BY references a column alias would be planned against the
-- un-aliased SELECT and fail with UNKNOWN_IDENTIFIER before the view could be created.

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

-- An ordinary view over the source table of a materialized view reads the whole table
-- also when the analyzer inlines the view body into the view query.

SET analyzer_inline_views = 1;

DROP TABLE IF EXISTS mv_scalar;
DROP TABLE IF EXISTS mv_join;
DROP TABLE IF EXISTS dst_scalar;
DROP TABLE IF EXISTS dst_join;
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS src;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE VIEW v AS SELECT x FROM src;
-- Rows inserted before the materialized views exist: the view sees them, the inserted block does not contain them.
INSERT INTO src SELECT number + 100 FROM numbers(10);

CREATE TABLE dst_scalar (block_rows UInt64, view_rows UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv_scalar TO dst_scalar AS
    SELECT count() AS block_rows, (SELECT count() FROM v) AS view_rows FROM src;

CREATE TABLE dst_join (x UInt64, view_rows UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_join TO dst_join AS
    SELECT s.x AS x, w.c AS view_rows FROM src AS s CROSS JOIN (SELECT count() AS c FROM v) AS w;

INSERT INTO src VALUES (1), (2), (3);

-- Whether the inserted block itself is already visible through the view depends on when the part is committed.
SELECT block_rows, view_rows >= 10 FROM dst_scalar;
SELECT x, view_rows >= 10 FROM dst_join ORDER BY x;

DROP TABLE mv_scalar;
DROP TABLE mv_join;
DROP TABLE dst_scalar;
DROP TABLE dst_join;
DROP VIEW v;
DROP TABLE src;

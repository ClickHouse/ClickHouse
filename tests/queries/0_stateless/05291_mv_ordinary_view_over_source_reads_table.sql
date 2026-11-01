-- The query of a materialized view sees only the inserted block of its source table,
-- but an ordinary view over the same table referenced from that query reads the whole table.

DROP TABLE IF EXISTS mv_scalar;
DROP TABLE IF EXISTS mv_join;
DROP TABLE IF EXISTS mv_inner;
DROP TABLE IF EXISTS dst_scalar;
DROP TABLE IF EXISTS dst_join;
DROP TABLE IF EXISTS dst_inner;
DROP VIEW IF EXISTS v;
DROP VIEW IF EXISTS v_in;
DROP VIEW IF EXISTS v_scalar;
DROP TABLE IF EXISTS src;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE VIEW v AS SELECT x FROM src;
-- The subqueries inside an ordinary view read the table as well.
CREATE VIEW v_in AS SELECT count() AS c FROM numbers(200) WHERE number IN (SELECT x FROM src);
CREATE VIEW v_scalar AS SELECT (SELECT count() FROM src) AS c;
-- Rows inserted before the materialized views exist: the view sees them, the inserted block does not contain them.
INSERT INTO src SELECT number + 100 FROM numbers(10);

CREATE TABLE dst_scalar (block_rows UInt64, view_rows UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv_scalar TO dst_scalar AS
    SELECT count() AS block_rows, (SELECT count() FROM v) AS view_rows FROM src;

CREATE TABLE dst_join (x UInt64, view_rows UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_join TO dst_join AS
    SELECT s.x AS x, w.c AS view_rows FROM src AS s CROSS JOIN (SELECT count() AS c FROM v) AS w;

CREATE TABLE dst_inner (block_rows UInt64, in_rows Nullable(UInt64), scalar_rows Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv_inner TO dst_inner AS
    SELECT count() AS block_rows, (SELECT c FROM v_in) AS in_rows, (SELECT c FROM v_scalar) AS scalar_rows FROM src;

INSERT INTO src VALUES (1), (2), (3);

-- Whether the inserted block itself is already visible through the view depends on when the part is committed.
SELECT block_rows, view_rows >= 10 FROM dst_scalar;
SELECT x, view_rows >= 10 FROM dst_join ORDER BY x;
SELECT block_rows, in_rows >= 10, scalar_rows >= 10 FROM dst_inner;

DROP TABLE mv_scalar;
DROP TABLE mv_join;
DROP TABLE mv_inner;
DROP TABLE dst_scalar;
DROP TABLE dst_join;
DROP TABLE dst_inner;
DROP VIEW v;
DROP VIEW v_in;
DROP VIEW v_scalar;
DROP TABLE src;

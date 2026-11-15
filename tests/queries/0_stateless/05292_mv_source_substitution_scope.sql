-- The pushed block stands in for the source table in the view query, including its subqueries.
-- A view over the source table keeps reading the whole table.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS v;
DROP TABLE IF EXISTS dst_scalar;
DROP TABLE IF EXISTS dst_join;
DROP TABLE IF EXISTS dst_subquery;
DROP TABLE IF EXISTS dst_in;
DROP TABLE IF EXISTS mv_scalar;
DROP TABLE IF EXISTS mv_join;
DROP TABLE IF EXISTS mv_subquery;
DROP TABLE IF EXISTS mv_in;

CREATE TABLE src (k UInt32) ENGINE = MergeTree ORDER BY k;
CREATE VIEW v AS SELECT count() AS c FROM src;

CREATE TABLE dst_scalar (k UInt32, c UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv_scalar TO dst_scalar AS SELECT k, (SELECT c FROM v) AS c FROM src;

CREATE TABLE dst_join (k UInt32, c UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv_join TO dst_join AS SELECT k, c FROM src CROSS JOIN v;

CREATE TABLE dst_subquery (k UInt32, c UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv_subquery TO dst_subquery AS SELECT k, c FROM src CROSS JOIN (SELECT count() AS c FROM src) AS t;

CREATE TABLE dst_in (k UInt32, c UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv_in TO dst_in AS SELECT k, c FROM src CROSS JOIN (SELECT count() AS c FROM numbers(10) WHERE number IN (SELECT k FROM src)) AS t;

INSERT INTO src VALUES (1), (2);
INSERT INTO src VALUES (3);

SELECT 'view in scalar subquery';
SELECT * FROM dst_scalar ORDER BY k;
SELECT 'view in join';
SELECT * FROM dst_join ORDER BY k;
SELECT 'source table in subquery';
SELECT * FROM dst_subquery ORDER BY k;
SELECT 'source table in IN';
SELECT * FROM dst_in ORDER BY k;

DROP TABLE mv_scalar;
DROP TABLE mv_join;
DROP TABLE mv_subquery;
DROP TABLE mv_in;
DROP TABLE dst_scalar;
DROP TABLE dst_join;
DROP TABLE dst_subquery;
DROP TABLE dst_in;
DROP TABLE v;
DROP TABLE src;

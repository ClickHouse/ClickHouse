-- The pushdown must not rewrite reads of the source table of a dependent materialized view:
-- at execution time that table is replaced with `StorageValues` holding the inserted block,
-- and the replacement happens after the query tree passes have run.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_view_source_src;
DROP TABLE IF EXISTS t_view_source_dst;
DROP TABLE IF EXISTS mv_view_source;

CREATE TABLE t_view_source_src (id UInt32, json JSON, tup Tuple(a UInt32, b String)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_view_source_dst (id UInt32, a UInt32, b String) ENGINE = MergeTree ORDER BY id;

CREATE MATERIALIZED VIEW mv_view_source TO t_view_source_dst AS
SELECT id, tup.a AS a, tup.b AS b FROM (SELECT * FROM t_view_source_src);

INSERT INTO t_view_source_src VALUES (1, '{"a": 1}', (10, 'ten')), (2, '{"a": 2}', (20, 'twenty'));

SELECT 'materialized view over a subquery';
SELECT * FROM t_view_source_dst ORDER BY id;

SELECT 'the source table itself';
SELECT id, tup.a, tup.b FROM (SELECT * FROM t_view_source_src) ORDER BY id;

DROP TABLE mv_view_source;
TRUNCATE TABLE t_view_source_dst;

SELECT 'JSON subcolumn in a materialized view over a CTE';
CREATE MATERIALIZED VIEW mv_view_source TO t_view_source_dst AS
WITH s AS (SELECT * FROM t_view_source_src) SELECT id, json.a::UInt32 AS a, tup.b AS b FROM s;

INSERT INTO t_view_source_src VALUES (3, '{"a": 3}', (30, 'thirty'));
SELECT * FROM t_view_source_dst ORDER BY id;

DROP TABLE mv_view_source;
DROP TABLE t_view_source_dst;
DROP TABLE t_view_source_src;

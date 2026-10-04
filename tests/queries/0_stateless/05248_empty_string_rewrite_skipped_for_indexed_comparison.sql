-- A key or skip index is matched to a query by its declared spelling, so the rewrite of `s = ''` stands down when a table
-- in the query declares one on such a comparison.
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET optimize_empty_string_comparisons = 1;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS mv_buf;
DROP TABLE IF EXISTS buf;
DROP TABLE IF EXISTS plain;

CREATE TABLE tab (s String, INDEX idx if(s = '', 'abc', s) TYPE minmax) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO tab VALUES (''), ('x'), ('y'), ('z');
SELECT count() FROM tab WHERE if(s = '', 'abc', s) = 'abc' SETTINGS force_data_skipping_indices = 'idx';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE if(s = '', 'abc', s) = 'abc') WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM tab WHERE if(s = '', 'abc', s) = 'abc') WHERE explain LIKE '%function_name: empty%';
-- The table may sit in a subquery, or next to a table without such an index.
SELECT count() FROM system.one WHERE 1 IN (SELECT count() FROM tab WHERE if(s = '', 'abc', s) = 'abc' SETTINGS force_data_skipping_indices = 'idx');
CREATE TABLE plain (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO plain VALUES (''), ('x');
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM plain, tab WHERE if(plain.s = '', 'abc', plain.s) = 'abc') WHERE explain LIKE '%function_name: empty%';
-- The index of the target table is seen through a materialized view, a `Buffer` table and a chain of both.
CREATE TABLE dst (s String, INDEX idx if(s = '', 'abc', s) TYPE minmax) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT s FROM plain;
CREATE TABLE buf AS dst ENGINE = Buffer(currentDatabase(), dst, 1, 1, 1, 1, 1, 1, 1);
CREATE MATERIALIZED VIEW mv_buf TO buf AS SELECT s FROM plain WHERE 0;
INSERT INTO plain VALUES (''), ('x'), ('y'), ('z');
SELECT count() FROM mv WHERE if(s = '', 'abc', s) = 'abc' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM buf WHERE if(s = '', 'abc', s) = 'abc' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM mv_buf WHERE if(s = '', 'abc', s) = 'abc' SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE tab;
DROP TABLE mv;
DROP TABLE mv_buf;
DROP TABLE buf;
DROP TABLE dst;

-- The same for a key, and for an index declared on an `ALIAS` column, whose expression is stored expanded.
CREATE TABLE tab (s String) ENGINE = MergeTree ORDER BY if(s = '', 'abc', s) SETTINGS index_granularity = 2;
INSERT INTO tab VALUES (''), ('x'), ('y'), ('z');
SELECT count() FROM tab WHERE if(s = '', 'abc', s) = 'abc' SETTINGS force_primary_key = 1;
DROP TABLE tab;
CREATE TABLE tab (s String, a String ALIAS if(s = '', 'abc', s), INDEX idx a TYPE minmax) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO tab VALUES (''), ('x'), ('y'), ('z');
SELECT count() FROM tab WHERE a = 'abc' SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE tab;

-- The comparison may be written as `!=` or with the literal on the left.
CREATE TABLE tab (s String, INDEX idx ('' != s) TYPE minmax) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO tab VALUES (''), ('x'), ('y'), ('z');
SELECT count() FROM tab WHERE '' != s SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE tab;

-- A query over other tables is still rewritten, also when the setting is enabled only on the query.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM plain WHERE if(s = '', 'abc', s) = 'abc') WHERE explain LIKE '%function_name: empty%';
SET optimize_empty_string_comparisons = 0;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM plain WHERE if(s = '', 'abc', s) = 'abc' SETTINGS optimize_empty_string_comparisons = 1) WHERE explain LIKE '%function_name: empty%';
DROP TABLE plain;

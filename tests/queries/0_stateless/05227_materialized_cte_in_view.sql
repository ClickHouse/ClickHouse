-- A MATERIALIZED CTE in a stored view definition used to be expanded into a plain subquery at each
-- reference when the definition was stored, so the CTE ran once per reference.
-- https://github.com/ClickHouse/ClickHouse/issues/113711
-- Every `same` column below compares rand64() taken from two references of the same CTE:
-- 1 means both references read the same materialization.

SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS src_113711, src2_113711, t2_113711, r_113711, dst_113711, dst_chain_113711, dst_forward_113711, dst_inner_113711, dst_pinned_113711, dst_self_113711, dst_bad_113711, src_final_113711;
DROP TABLE IF EXISTS v_113711, v_settings_113711, pv_113711, v_self_113711, v_chain_113711, v_in_113711, v_in_chain_113711, v_forward_113711, v_union_113711, v_union2_113711, v_sibling_113711, v_nested_113711, v_copy_113711, v_baked_113711;
DROP TABLE IF EXISTS mv_113711, mv_chain_113711, mv_forward_113711, mv_inner_113711, mv_pinned_113711, mv_self_113711, mv_bad_113711;

CREATE TABLE src_113711 (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO src_113711 VALUES (1), (2), (3);

SELECT '-- view: both join sides read one materialization';
CREATE VIEW v_113711 AS
WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT a.id AS id, a.x = b.x AS same FROM r AS a INNER JOIN r AS b ON a.id = b.id;
SELECT * FROM v_113711 ORDER BY id;
SELECT countIf(explain LIKE '%MaterializingCTE%') > 0 FROM viewExplain('EXPLAIN', '', (SELECT * FROM v_113711));
SELECT replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH') FROM system.tables WHERE database = currentDatabase() AND name = 'v_113711';

SELECT '-- the same through analyzer_inline_views';
SELECT * FROM v_113711 ORDER BY id SETTINGS analyzer_inline_views = 1;

SELECT '-- the setting inside the definition is enough';
SET enable_materialized_cte = 0;
CREATE VIEW v_settings_113711 AS
WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT a.id AS id, a.x = b.x AS same FROM r AS a INNER JOIN r AS b ON a.id = b.id
SETTINGS enable_materialized_cte = 1;
SELECT * FROM v_settings_113711 ORDER BY id;

SELECT '-- with the setting disabled the keyword is ignored, as in a plain query';
SELECT * FROM v_113711 ORDER BY id SETTINGS send_logs_level = 'fatal';
SET enable_materialized_cte = 1;

SELECT '-- parameterized view';
CREATE VIEW pv_113711 AS
WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711 WHERE id >= {min_id:UInt32})
SELECT a.id AS id, a.x = b.x AS same FROM r AS a INNER JOIN r AS b ON a.id = b.id;
SELECT * FROM pv_113711(min_id = 2) ORDER BY id;

SELECT '-- CTE named like its source table: the body reads the table, the query reads the CTE';
CREATE VIEW v_self_113711 AS
WITH src_113711 AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT a.id AS id, a.x = b.x AS same FROM src_113711 AS a INNER JOIN src_113711 AS b ON a.id = b.id;
SELECT replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH') FROM system.tables WHERE database = currentDatabase() AND name = 'v_self_113711';
SELECT * FROM v_self_113711 ORDER BY id;

SELECT '-- a materialized CTE over another one, and a plain one over a materialized one (expanded copy carries the reference)';
CREATE VIEW v_chain_113711 AS
WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711),
     q AS MATERIALIZED (SELECT id, x FROM r),
     p AS (SELECT id, x FROM q)
SELECT a.id AS id, a.x = b.x AS same FROM p AS a INNER JOIN r AS b ON a.id = b.id;
SELECT * FROM v_chain_113711 ORDER BY id;
SELECT replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH') FROM system.tables WHERE database = currentDatabase() AND name = 'v_chain_113711';

SELECT '-- forward reference: the materialized CTE is declared after the one that uses it';
CREATE VIEW v_forward_113711 AS
WITH b AS MATERIALIZED (SELECT id, x FROM a),
     a AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT b1.id AS id, b1.x = a1.x AS same FROM b AS b1 INNER JOIN a AS a1 ON b1.id = a1.id;
SELECT * FROM v_forward_113711 ORDER BY id;
SELECT replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH') FROM system.tables WHERE database = currentDatabase() AND name = 'v_forward_113711';

SELECT '-- reference through IN';
CREATE VIEW v_in_113711 AS
WITH r AS MATERIALIZED (SELECT id FROM src_113711 WHERE id > 1)
SELECT count() AS c FROM src_113711 WHERE id IN r AND id IN (SELECT id FROM r);
SELECT * FROM v_in_113711;
SELECT replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH') FROM system.tables WHERE database = currentDatabase() AND name = 'v_in_113711';

SELECT '-- a plain CTE carrying a materialized reference, expanded through IN';
CREATE VIEW v_in_chain_113711 AS
WITH q AS MATERIALIZED (SELECT id FROM src_113711 WHERE id > 1),
     p AS (SELECT id FROM q)
SELECT count() AS c FROM src_113711 WHERE id IN p AND id IN (SELECT id FROM q);
SELECT * FROM v_in_chain_113711;
SELECT replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH') FROM system.tables WHERE database = currentDatabase() AND name = 'v_in_chain_113711';

SELECT '-- UNION: the WITH list is copied into every branch and merged back into one materialization';
CREATE VIEW v_union_113711 AS
WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT a.x = b.x AS same FROM r AS a INNER JOIN r AS b ON a.id = b.id
UNION ALL
SELECT a.x = b.x FROM r AS a INNER JOIN r AS b ON a.id = b.id;
SELECT same, count() FROM v_union_113711 GROUP BY same;

SELECT '-- UNION branches declaring same-named materialized CTEs with different bodies, named like the source table';
CREATE VIEW v_union2_113711 AS
WITH src_113711 AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT 'a' AS branch, a.x = b.x AS same FROM src_113711 AS a INNER JOIN src_113711 AS b ON a.id = b.id
UNION ALL
WITH src_113711 AS MATERIALIZED (SELECT id + 100 AS id, rand64() AS x FROM src_113711)
SELECT 'b', a.x = b.x FROM src_113711 AS a INNER JOIN src_113711 AS b ON a.id = b.id;
SELECT branch, same, count() FROM v_union2_113711 GROUP BY branch, same ORDER BY branch;
SELECT replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH') FROM system.tables WHERE database = currentDatabase() AND name = 'v_union2_113711';

SELECT '-- a materialized CTE of one subquery does not hide a same-named table in a sibling subquery';
CREATE TABLE r_113711 (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO r_113711 SELECT number FROM numbers(7);
CREATE VIEW v_sibling_113711 AS
SELECT
    (SELECT count() FROM (WITH r_113711 AS MATERIALIZED (SELECT id FROM src_113711) SELECT a.id FROM r_113711 AS a INNER JOIN r_113711 AS b ON a.id = b.id)) AS from_cte,
    (SELECT count() FROM r_113711) AS from_table;
SELECT * FROM v_sibling_113711;
SELECT replaceRegexpOne(create_table_query, '.*AS SELECT', 'SELECT') FROM system.tables WHERE database = currentDatabase() AND name = 'v_sibling_113711';

SELECT '-- enable_global_with_statement = 0: references in the declaring SELECT work, a nested one fails like in a plain query';
SELECT * FROM v_113711 ORDER BY id SETTINGS enable_global_with_statement = 0;
CREATE VIEW v_nested_113711 AS
WITH r AS MATERIALIZED (SELECT id FROM src_113711)
SELECT count() AS c FROM r WHERE id IN (SELECT id FROM r);
SELECT * FROM v_nested_113711;
SELECT * FROM v_nested_113711 SETTINGS enable_global_with_statement = 0; -- { serverError UNKNOWN_TABLE }
WITH r AS MATERIALIZED (SELECT id FROM src_113711) SELECT count() AS c FROM r WHERE id IN (SELECT id FROM r) SETTINGS enable_global_with_statement = 0; -- { serverError UNKNOWN_TABLE }

SELECT '-- DETACH / ATTACH reloads the stored definition';
DETACH TABLE v_113711;
ATTACH TABLE v_113711;
SELECT * FROM v_113711 ORDER BY id;

SELECT '-- materialized view: only the inserted block is read, once; also after MODIFY QUERY';
CREATE TABLE dst_113711 (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_113711 TO dst_113711 AS
WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT a.id AS id, a.x = b.x AS same FROM r AS a INNER JOIN r AS b ON a.id = b.id;
INSERT INTO src_113711 VALUES (10), (11);
SELECT * FROM dst_113711 ORDER BY id;
ALTER TABLE mv_113711 MODIFY QUERY
WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT a.id AS id, a.x = b.x AS same FROM r AS a INNER JOIN r AS b ON a.id = b.id WHERE a.id > 20;
INSERT INTO src_113711 VALUES (21), (22);
SELECT * FROM dst_113711 ORDER BY id;
DROP TABLE mv_113711;

SELECT '-- materialized view fed through two chained materialized CTEs';
CREATE TABLE dst_chain_113711 (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_chain_113711 TO dst_chain_113711 AS
WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711),
     q AS MATERIALIZED (SELECT id, x FROM r)
SELECT a.id AS id, a.x = b.x AS same FROM q AS a INNER JOIN r AS b ON a.id = b.id;
INSERT INTO src_113711 VALUES (31), (32);
SELECT * FROM dst_chain_113711 ORDER BY id;
DROP TABLE mv_chain_113711;

SELECT '-- materialized view whose first table is a forward-referenced materialized CTE';
CREATE TABLE dst_forward_113711 (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_forward_113711 TO dst_forward_113711 AS
WITH b AS MATERIALIZED (SELECT id, x FROM a),
     a AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT b1.id AS id, b1.x = a1.x AS same FROM b AS b1 INNER JOIN a AS a1 ON b1.id = a1.id;
INSERT INTO src_113711 VALUES (41), (42);
SELECT * FROM dst_forward_113711 ORDER BY id;
DROP TABLE mv_forward_113711;

SELECT '-- materialized view: an inner same-named plain CTE whose body reads the enclosing materialized CTE';
CREATE TABLE dst_inner_113711 (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_inner_113711 TO dst_inner_113711 AS
WITH r AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT id, same FROM (WITH r AS (SELECT id, x FROM r) SELECT a.id AS id, a.x = b.x AS same FROM r AS a INNER JOIN r AS b ON a.id = b.id);
INSERT INTO src_113711 VALUES (51), (52);
SELECT * FROM dst_inner_113711 ORDER BY id;
DROP TABLE mv_inner_113711;

SELECT '-- materialized view: enable_global_with_statement is pinned, a same-named table does not capture a nested reference';
CREATE TABLE dst_pinned_113711 (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_pinned_113711 TO dst_pinned_113711 AS
WITH r_113711 AS MATERIALIZED (SELECT id, rand64() AS x FROM src_113711)
SELECT a.id AS id, a.x = b.x AS same FROM (SELECT id, x FROM r_113711) AS a INNER JOIN r_113711 AS b ON a.id = b.id;
INSERT INTO src_113711 SETTINGS enable_global_with_statement = 0 VALUES (61), (62);
SELECT * FROM dst_pinned_113711 ORDER BY id;
DROP TABLE mv_pinned_113711;

SELECT '-- the setting cannot be fixed in a fresh materialized view definition';
CREATE TABLE dst_bad_113711 (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_bad_113711 TO dst_bad_113711 AS WITH r AS MATERIALIZED (SELECT id FROM src_113711) SELECT id FROM r SETTINGS enable_global_with_statement = 0; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW mv_bad_113711 TO dst_bad_113711 AS SELECT id FROM src_113711;
ALTER TABLE mv_bad_113711 MODIFY QUERY WITH r AS MATERIALIZED (SELECT id FROM src_113711) SELECT id FROM r SETTINGS enable_global_with_statement = 1; -- { serverError NOT_IMPLEMENTED }
SELECT replaceRegexpOne(create_table_query, '.*AS SELECT', 'SELECT') FROM system.tables WHERE database = currentDatabase() AND name = 'mv_bad_113711';
DROP TABLE mv_bad_113711;

SELECT '-- PREWHERE, SAMPLE and FINAL inside the CTE are still rejected in a materialized view definition';
CREATE TABLE src_final_113711 (id UInt32) ENGINE = ReplacingMergeTree ORDER BY id SAMPLE BY id;
CREATE MATERIALIZED VIEW mv_bad_113711 TO dst_bad_113711 AS WITH r AS MATERIALIZED (SELECT id FROM src_final_113711 FINAL) SELECT id FROM r; -- { serverError QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW }
CREATE MATERIALIZED VIEW mv_bad_113711 TO dst_bad_113711 AS WITH r AS MATERIALIZED (SELECT id FROM src_final_113711 PREWHERE id > 0) SELECT id FROM r; -- { serverError QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW }
CREATE MATERIALIZED VIEW mv_bad_113711 TO dst_bad_113711 AS WITH r AS MATERIALIZED (SELECT id FROM src_final_113711 SAMPLE 1/2) SELECT id FROM r; -- { serverError QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW }
CREATE MATERIALIZED VIEW mv_bad_113711 TO dst_bad_113711 AS WITH b AS MATERIALIZED (SELECT id FROM a), a AS MATERIALIZED (SELECT id FROM src_final_113711 FINAL) SELECT id FROM b; -- { serverError QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW }

SELECT '-- a CTE-backed first UNION arm does not hide FINAL in a later arm';
CREATE MATERIALIZED VIEW mv_bad_113711 TO dst_bad_113711 AS WITH r AS MATERIALIZED (SELECT id FROM src_final_113711) SELECT id FROM r UNION ALL SELECT id FROM src_final_113711 FINAL; -- { serverError QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW }

SELECT '-- materialized view with a CTE named like its source table: only the inserted block is read, once';
CREATE TABLE src2_113711 (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO src2_113711 VALUES (1), (2), (3);
CREATE TABLE dst_self_113711 (id UInt32, same UInt8) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_self_113711 TO dst_self_113711 AS
WITH src2_113711 AS MATERIALIZED (SELECT id, rand64() AS x FROM src2_113711)
SELECT a.id AS id, a.x = b.x AS same FROM src2_113711 AS a INNER JOIN src2_113711 AS b ON a.id = b.id;
INSERT INTO src2_113711 VALUES (71), (72);
SELECT * FROM dst_self_113711 ORDER BY id;
DROP TABLE mv_self_113711;

SELECT '-- an expansion copy of a plain CTE is classified where it stands, like the analyzer at run time';
-- `p`'s body reads `t2_113711` as a table, but the reference site (inside the outer `SELECT`) declares
-- a `MATERIALIZED` CTE named `t2_113711`. The analyzer resolves `p`'s body at that reference site, so
-- the expansion copy of `p` reads the `MATERIALIZED` CTE, not the table `t2_113711`.
CREATE TABLE t2_113711 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t2_113711 VALUES (1), (2), (3);
CREATE VIEW v_copy_113711 AS
WITH p AS (SELECT * FROM t2_113711)
SELECT * FROM (WITH t2_113711 AS MATERIALIZED (SELECT 1 AS x) SELECT * FROM p);
SELECT * FROM v_copy_113711 ORDER BY x;
SELECT replaceRegexpOne(create_table_query, '.*AS WITH', 'WITH') FROM system.tables WHERE database = currentDatabase() AND name = 'v_copy_113711';

SELECT '-- a view definition that fixes enable_global_with_statement bakes in the nested-reference failure';
CREATE VIEW v_baked_113711 AS
WITH r AS MATERIALIZED (SELECT 1 AS x)
SELECT * FROM (SELECT * FROM r)
SETTINGS enable_global_with_statement = 0; -- { serverError UNKNOWN_TABLE }

DROP TABLE v_copy_113711, v_nested_113711, v_sibling_113711, v_union2_113711, v_union_113711, v_in_chain_113711, v_in_113711, v_forward_113711, v_chain_113711, v_self_113711, pv_113711, v_settings_113711, v_113711;
DROP TABLE src_final_113711, dst_bad_113711, dst_self_113711, dst_pinned_113711, dst_inner_113711, dst_forward_113711, dst_chain_113711, dst_113711, t2_113711, r_113711, src2_113711, src_113711;

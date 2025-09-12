-- Test for LikeToRangeRewritePass optimization in analyzer
-- This pass should rewrite LIKE expressions to range conditions.

SET enable_analyzer = 1;
SET optimize_rewrite_like_to_range = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id UInt32,
    col_string String,
    col_fixedstring FixedString(10),
    col_lowcardinality_string LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY col_string;

INSERT INTO tab VALUES 
    (1, 'apple', 'fruit', 'aaa'),
    (2, 'application', 'software', 'bbb'),
    (3, 'apply', 'verb', 'ccc'),
    (4, 'banana', 'fruit', 'aaa'),
    (5, 'band', 'music', 'bbb'),
    (6, 'Test', 'other', 'ccc'),
    (7, 'Testing', 'other', 'aaa');

SELECT 'Test LIKE with perfect prefix patterns - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE 'app%';
SELECT count() FROM tab WHERE col_string LIKE 'app%';
SELECT count() FROM tab WHERE col_string LIKE 'app%' SETTINGS optimize_rewrite_like_to_range = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE 'Test%';
SELECT count() FROM tab WHERE col_string LIKE 'Test%';
SELECT count() FROM tab WHERE col_string LIKE 'Test%' SETTINGS optimize_rewrite_like_to_range = 0;

SELECT 'Test LIKE with FixedString column - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_fixedstring LIKE 'fruit%';
SELECT count() FROM tab WHERE col_fixedstring LIKE 'fruit%';
SELECT count() FROM tab WHERE col_fixedstring LIKE 'fruit%' SETTINGS optimize_rewrite_like_to_range = 0;

SELECT 'Test NOT LIKE with perfect prefix - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT LIKE 'app%';
SELECT count() FROM tab WHERE col_string NOT LIKE 'app%';
SELECT count() FROM tab WHERE col_string NOT LIKE 'app%' SETTINGS optimize_rewrite_like_to_range = 0;

SELECT 'Test ILIKE with perfect prefix - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string ILIKE 'APP%';
SELECT count() FROM tab WHERE col_string ILIKE 'APP%';
SELECT count() FROM tab WHERE col_string ILIKE 'APP%' SETTINGS optimize_rewrite_like_to_range = 0;

SELECT 'Test multiple LIKE conditions - all eligible ones should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE 'app%' AND col_fixedstring LIKE 'fruit%';
SELECT count() FROM tab WHERE col_string LIKE 'app%' AND col_fixedstring LIKE 'fruit%';
SELECT count() FROM tab WHERE col_string LIKE 'app%' AND col_fixedstring LIKE 'fruit%' SETTINGS optimize_rewrite_like_to_range = 0;

SELECT 'Test without prefix patterns - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string LIKE 'app_ication%' SETTINGS optimize_rewrite_like_to_range = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE '%app%';
SELECT count() FROM tab WHERE col_string LIKE '%app%';
SELECT count() FROM tab WHERE col_string LIKE '%app%' SETTINGS optimize_rewrite_like_to_range = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE '_est%';
SELECT count() FROM tab WHERE col_string LIKE '_est%';
SELECT count() FROM tab WHERE col_string LIKE '_est%' SETTINGS optimize_rewrite_like_to_range = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE 'Test';
SELECT count() FROM tab WHERE col_string LIKE 'Test';
SELECT count() FROM tab WHERE col_string LIKE 'Test' SETTINGS optimize_rewrite_like_to_range = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE '%';
SELECT count() FROM tab WHERE col_string LIKE '%';
SELECT count() FROM tab WHERE col_string LIKE '%' SETTINGS optimize_rewrite_like_to_range = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string NOT LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string NOT LIKE 'app_ication%' SETTINGS optimize_rewrite_like_to_range = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string ILIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string ILIKE 'app_ication%' SETTINGS optimize_rewrite_like_to_range = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string NOT ILIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string NOT ILIKE 'app_ication%' SETTINGS optimize_rewrite_like_to_range = 0;

SELECT 'Test low cardinality attribute - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_lowcardinality_string LIKE 'a%';
SELECT count() from tab WHERE col_lowcardinality_string LIKE 'a%';
SELECT count() from tab WHERE col_lowcardinality_string LIKE 'a%' SETTINGS optimize_rewrite_like_to_range = 0;

DROP TABLE tab;

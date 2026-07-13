-- Test for LikePerfectAffixRewritePass optimization in analyzer

SET enable_analyzer = 1;
SET optimize_rewrite_like_perfect_affix = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id UInt32,
    col_string String,
    col_fixedstring FixedString(32),
    col_lowcardinality_string LowCardinality(String),
    col_lowcardinality_fixedstring LowCardinality(FixedString(32)),
    col_nullable_string Nullable(String),
    col_nullable_fixedstring Nullable(FixedString(32)),
    col_lowcardinality_nullable_string LowCardinality(Nullable(String)),
    col_lowcardinality_nullable_fixedstring LowCardinality(Nullable(FixedString(32)))
) ENGINE = MergeTree()
ORDER BY col_string;

INSERT INTO tab VALUES 
    (1, 'apple', 'fruit', 'aaa', 'aaa', 'aaa', 'aaa', 'aaa', 'aaa'),
    (2, 'application', 'software', 'bbb', 'bbb', 'bbb', 'bbb', 'bbb', 'bbb'),
    (3, 'apply', 'verb', 'ccc', 'ccc', 'ccc', 'ccc', 'ccc', 'ccc'),
    (4, 'banana', 'fruit', 'aaa', 'aaa', 'aaa', 'aaa', 'aaa', 'aaa'),
    (5, 'band', 'music', 'bbb', 'bbb', 'bbb', 'bbb', 'bbb', 'bbb'),
    (6, 'Test', 'other', 'ccc', 'ccc', 'ccc', 'ccc', 'ccc', 'ccc'),
    (7, 'A-Test', 'another', 'aaa', 'aaa', 'aaa', 'aaa', 'aaa', 'aaa');

SELECT '-- Test LIKE perfect prefix on String column - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE 'app%';
SELECT count() FROM tab WHERE col_string LIKE 'app%';
SELECT count() FROM tab WHERE col_string LIKE 'app%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
SELECT '-- Test LIKE perfect suffix on String column - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE '%Test';
SELECT count() FROM tab WHERE col_string LIKE '%Test';
SELECT count() FROM tab WHERE col_string LIKE '%Test' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test LIKE perfect prefix on FixedString column - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_fixedstring LIKE 'fruit%';
SELECT count() FROM tab WHERE col_fixedstring LIKE 'fruit%';
SELECT count() FROM tab WHERE col_fixedstring LIKE 'fruit%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
SELECT '-- Test LIKE perfect suffix on FixedString column - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_fixedstring LIKE '%ther\0';
SELECT count() FROM tab WHERE col_fixedstring LIKE '%ther\0';
SELECT count() FROM tab WHERE col_fixedstring LIKE '%ther\0' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test NOT LIKE with perfect prefix on String column - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT LIKE 'app%';
SELECT count() FROM tab WHERE col_string NOT LIKE 'app%';
SELECT count() FROM tab WHERE col_string NOT LIKE 'app%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
SELECT '-- Test NOT LIKE with perfect suffix on String column - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT LIKE '%Test';
SELECT count() FROM tab WHERE col_string NOT LIKE '%Test';
SELECT count() FROM tab WHERE col_string NOT LIKE '%Test' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test ILIKE with perfect prefix on String column - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string ILIKE 'APP%';
SELECT count() FROM tab WHERE col_string ILIKE 'APP%';
SELECT count() FROM tab WHERE col_string ILIKE 'APP%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
SELECT '-- Test ILIKE with perfect suffix on String column - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string ILIKE '%TeST';
SELECT count() FROM tab WHERE col_string ILIKE '%TeST';
SELECT count() FROM tab WHERE col_string ILIKE '%TeST' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test NOT ILIKE with perfect prefix on String column - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT ILIKE 'APP%';
SELECT count() FROM tab WHERE col_string NOT ILIKE 'APP%';
SELECT count() FROM tab WHERE col_string NOT ILIKE 'APP%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
SELECT '-- Test NOT ILIKE with perfect suffix on String column - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT ILIKE '%TeST';
SELECT count() FROM tab WHERE col_string NOT ILIKE '%TeST';
SELECT count() FROM tab WHERE col_string NOT ILIKE '%TeST' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test multiple LIKE conditions - all eligible ones should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE 'app%' AND col_fixedstring LIKE 'fruit%';
SELECT count() FROM tab WHERE col_string LIKE 'app%' AND col_fixedstring LIKE 'fruit%';
SELECT count() FROM tab WHERE col_string LIKE 'app%' AND col_fixedstring LIKE 'fruit%' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test without perfect affix - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string LIKE 'app_ication%' SETTINGS optimize_rewrite_like_perfect_affix = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE '%app_ication';
SELECT count() FROM tab WHERE col_string LIKE '%app_ication';
SELECT count() FROM tab WHERE col_string LIKE '%app_ication' SETTINGS optimize_rewrite_like_perfect_affix = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE '%app%';
SELECT count() FROM tab WHERE col_string LIKE '%app%';
SELECT count() FROM tab WHERE col_string LIKE '%app%' SETTINGS optimize_rewrite_like_perfect_affix = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE '_est%';
SELECT count() FROM tab WHERE col_string LIKE '_est%';
SELECT count() FROM tab WHERE col_string LIKE '_est%' SETTINGS optimize_rewrite_like_perfect_affix = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE 'Test';
SELECT count() FROM tab WHERE col_string LIKE 'Test';
SELECT count() FROM tab WHERE col_string LIKE 'Test' SETTINGS optimize_rewrite_like_perfect_affix = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string LIKE '%';
SELECT count() FROM tab WHERE col_string LIKE '%';
SELECT count() FROM tab WHERE col_string LIKE '%' SETTINGS optimize_rewrite_like_perfect_affix = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string NOT LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string NOT LIKE 'app_ication%' SETTINGS optimize_rewrite_like_perfect_affix = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string ILIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string ILIKE 'app_ication%' SETTINGS optimize_rewrite_like_perfect_affix = 0;

EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_string NOT LIKE 'app_ication%';
SELECT count() FROM tab WHERE col_string NOT ILIKE '%app_ication';
SELECT count() FROM tab WHERE col_string NOT ILIKE '%app_ication' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test low cardinality string column - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_lowcardinality_string LIKE 'a%';
SELECT count() from tab WHERE col_lowcardinality_string LIKE 'a%';
SELECT count() from tab WHERE col_lowcardinality_string LIKE 'a%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_lowcardinality_string LIKE '%a';
SELECT count() from tab WHERE col_lowcardinality_string LIKE '%a';
SELECT count() from tab WHERE col_lowcardinality_string LIKE '%a' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test nullable string column - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_nullable_string LIKE 'a%';
SELECT count() from tab WHERE col_nullable_string LIKE 'a%';
SELECT count() from tab WHERE col_nullable_string LIKE 'a%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_nullable_string LIKE '%a';
SELECT count() from tab WHERE col_nullable_string LIKE '%a';
SELECT count() from tab WHERE col_nullable_string LIKE '%a' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test low cardinality nullable string column - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_lowcardinality_nullable_string LIKE 'a%';
SELECT count() from tab WHERE col_lowcardinality_nullable_string LIKE 'a%';
SELECT count() from tab WHERE col_lowcardinality_nullable_string LIKE 'a%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_lowcardinality_nullable_string LIKE '%a';
SELECT count() from tab WHERE col_lowcardinality_nullable_string LIKE '%a';
SELECT count() from tab WHERE col_lowcardinality_nullable_string LIKE '%a' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test low cardinality fixedstring with prefix - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_lowcardinality_fixedstring LIKE 'a%';
SELECT count() from tab WHERE col_lowcardinality_fixedstring LIKE 'a%';
SELECT count() from tab WHERE col_lowcardinality_fixedstring LIKE 'a%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
SELECT '-- Test low cardinality fixedstring with suffix - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_lowcardinality_fixedstring LIKE '%a\0';
SELECT count() from tab WHERE col_lowcardinality_fixedstring LIKE '%a\0';
SELECT count() from tab WHERE col_lowcardinality_fixedstring LIKE '%a\0' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test nullable fixedstring with prefix - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_nullable_fixedstring LIKE 'a%';
SELECT count() from tab WHERE col_nullable_fixedstring LIKE 'a%';
SELECT count() from tab WHERE col_nullable_fixedstring LIKE 'a%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
SELECT '-- Test nullable fixedstring with suffix - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_nullable_fixedstring LIKE '%a\0';
SELECT count() from tab WHERE col_nullable_fixedstring LIKE '%a\0';
SELECT count() from tab WHERE col_nullable_fixedstring LIKE '%a\0' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- Test low cardinality nullable fixedstring with prefix - should be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_lowcardinality_nullable_fixedstring LIKE 'a%';
SELECT count() from tab WHERE col_lowcardinality_nullable_fixedstring LIKE 'a%';
SELECT count() from tab WHERE col_lowcardinality_nullable_fixedstring LIKE 'a%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
SELECT '-- Test low cardinality nullable fixedstring with suffix - should NOT be rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE col_lowcardinality_nullable_fixedstring LIKE '%a\0';
SELECT count() from tab WHERE col_lowcardinality_nullable_fixedstring LIKE '%a\0';
SELECT count() from tab WHERE col_lowcardinality_nullable_fixedstring LIKE '%a\0' SETTINGS optimize_rewrite_like_perfect_affix = 0;

DROP TABLE tab;

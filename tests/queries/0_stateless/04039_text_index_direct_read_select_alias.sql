-- Test that text index direct read works when predicate is also referenced in SELECT via alias.
-- Bug: the optimization renamed the filter column to a virtual column name, but the
-- downstream ExpressionStep still expected the original column name.

SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS t_text_idx_alias;

CREATE TABLE t_text_idx_alias
(
    `id` UInt32,
    `title` String,
    INDEX title_idx title TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_text_idx_alias VALUES (1, 'aaa'), (2, 'test'), (3, 'hello world');

-- This query failed with NOT_FOUND_COLUMN_IN_BLOCK because the predicate
-- appears both in WHERE (via alias x) and in SELECT.
SELECT id, hasAllTokens(title, 'test') AS x FROM t_text_idx_alias WHERE x ORDER BY id;
SELECT id, hasAllTokens(title, 'test') AS x FROM t_text_idx_alias WHERE hasAllTokens(title, 'test') ORDER BY id;

-- Also test with hasToken and hasAnyTokens
SELECT id, hasToken(title, 'aaa') AS y FROM t_text_idx_alias WHERE y;
SELECT id, hasToken(title, 'aaa') AS y FROM t_text_idx_alias WHERE hasToken(title, 'aaa') ORDER BY id;

SELECT id, hasAnyTokens(title, 'hello world') AS z FROM t_text_idx_alias WHERE z;
SELECT id, hasAnyTokens(title, 'hello world') AS z FROM t_text_idx_alias WHERE hasAnyTokens(title, 'hello world') ORDER BY id;

-- Test that the predicate value is correct in the output
SELECT id, hasAllTokens(title, ['nonexistent']) AS x FROM t_text_idx_alias WHERE NOT x ORDER BY id;

DROP TABLE t_text_idx_alias;

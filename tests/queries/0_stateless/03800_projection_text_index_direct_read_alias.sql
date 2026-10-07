SET allow_experimental_projection_text_index = 1;
-- Tests that projection text index direct read works when the filter predicate
-- is also referenced in SELECT via alias.
-- Adapted from 04039_text_index_direct_read_select_alias.

SET query_plan_direct_read_from_text_index = 1;
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_proj_alias;

CREATE TABLE t_proj_alias
(
    id UInt32,
    title String,
    PROJECTION idx INDEX title TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_proj_alias VALUES (1, 'aaa'), (2, 'test'), (3, 'hello world');

-- Predicate appears both in WHERE (via alias x) and in SELECT.
SELECT id, hasAllTokens(title, 'test') AS x FROM t_proj_alias WHERE x ORDER BY id;
SELECT id, hasAllTokens(title, 'test') AS x FROM t_proj_alias WHERE hasAllTokens(title, 'test') ORDER BY id;

-- hasToken variant
SELECT id, hasToken(title, 'aaa') AS y FROM t_proj_alias WHERE y;
SELECT id, hasToken(title, 'aaa') AS y FROM t_proj_alias WHERE hasToken(title, 'aaa') ORDER BY id;

-- hasAnyTokens variant
SELECT id, hasAnyTokens(title, 'hello world') AS z FROM t_proj_alias WHERE z;
SELECT id, hasAnyTokens(title, 'hello world') AS z FROM t_proj_alias WHERE hasAnyTokens(title, 'hello world') ORDER BY id;

-- Negated predicate in output
SELECT id, hasAllTokens(title, ['nonexistent']) AS x FROM t_proj_alias WHERE NOT x ORDER BY id;

DROP TABLE t_proj_alias;

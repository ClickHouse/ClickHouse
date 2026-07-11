-- Test that `query_plan_direct_read_from_text_index` works when the text-index
-- predicate is referenced via an alias in `SELECT` and used in `PREWHERE`.
-- PR #99504 fixed `NOT_FOUND_COLUMN_IN_BLOCK` for this pattern.

SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS t_04042_ti;

CREATE TABLE t_04042_ti
(
    id    UInt32,
    text  String,
    INDEX idx text TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_04042_ti VALUES (1, 'hello world'), (2, 'test data'), (3, 'foo bar'), (4, 'test hello');

-- PREWHERE alias defined in SELECT
SELECT id, hasToken(text, 'test') AS x FROM t_04042_ti PREWHERE x ORDER BY id;

-- Explicit predicate in PREWHERE, same expression aliased in SELECT
SELECT id, hasToken(text, 'test') AS x FROM t_04042_ti PREWHERE hasToken(text, 'test') ORDER BY id;

-- PREWHERE + WHERE with alias
SELECT id, hasToken(text, 'hello') AS y FROM t_04042_ti PREWHERE y WHERE id > 1 ORDER BY id;

-- hasAllTokens with PREWHERE alias
SELECT id, hasAllTokens(text, 'test data') AS z FROM t_04042_ti PREWHERE z ORDER BY id;

-- hasAnyTokens with PREWHERE alias
SELECT id, hasAnyTokens(text, 'test foo') AS w FROM t_04042_ti PREWHERE w ORDER BY id;

-- NOT predicate with PREWHERE alias
SELECT id, hasToken(text, 'test') AS x FROM t_04042_ti PREWHERE NOT x ORDER BY id;

DROP TABLE IF EXISTS t_04042_ti;

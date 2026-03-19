-- Test: text_index_lazy_apply_mode
-- Verifies that lazy posting list apply mode produces the same results as materialize mode.

DROP TABLE IF EXISTS t_text_idx_lazy;

CREATE TABLE t_text_idx_lazy
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128;

-- Insert enough data to create multiple segments.
INSERT INTO t_text_idx_lazy
SELECT
    number,
    concat(
        'The quick brown fox jumps over the lazy dog. ',
        'Document number ', toString(number), ' contains ',
        if(number % 3 = 0, 'alpha ', ''),
        if(number % 5 = 0, 'beta ', ''),
        if(number % 7 = 0, 'gamma ', ''),
        if(number % 11 = 0, 'delta ', ''),
        'end'
    )
FROM numbers(10000);

-- Force the index to be built with V2 format (.idx2).
OPTIMIZE TABLE t_text_idx_lazy FINAL;

-- Query 1: hasToken with materialize mode (baseline).
SELECT 'hasToken materialize';
SELECT count()
FROM t_text_idx_lazy
WHERE hasToken(text, 'alpha')
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

-- Query 2: hasToken with lazy mode.
SELECT 'hasToken lazy';
SELECT count()
FROM t_text_idx_lazy
WHERE hasToken(text, 'alpha')
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 3: hasAllTokens with materialize mode.
SELECT 'hasAllTokens materialize';
SELECT count()
FROM t_text_idx_lazy
WHERE hasAllTokens(text, ['alpha', 'beta'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

-- Query 4: hasAllTokens with lazy mode.
SELECT 'hasAllTokens lazy';
SELECT count()
FROM t_text_idx_lazy
WHERE hasAllTokens(text, ['alpha', 'beta'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 5: hasAnyTokens with materialize mode.
SELECT 'hasAnyTokens materialize';
SELECT count()
FROM t_text_idx_lazy
WHERE hasAnyTokens(text, ['alpha', 'beta', 'gamma'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

-- Query 6: hasAnyTokens with lazy mode.
SELECT 'hasAnyTokens lazy';
SELECT count()
FROM t_text_idx_lazy
WHERE hasAnyTokens(text, ['alpha', 'beta', 'gamma'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 7: Token not in index — both modes should return 0.
SELECT 'missing token materialize';
SELECT count()
FROM t_text_idx_lazy
WHERE hasToken(text, 'nonexistent_token_xyz')
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

SELECT 'missing token lazy';
SELECT count()
FROM t_text_idx_lazy
WHERE hasToken(text, 'nonexistent_token_xyz')
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

DROP TABLE t_text_idx_lazy;

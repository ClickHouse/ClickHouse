-- Test: text_index_lazy_extreme_selectivity
-- Verifies that lazy posting list apply mode produces correct results under extreme
-- selectivity differences (dense vs ultrarare tokens) with 1M rows.

SET allow_experimental_text_index_lazy_apply = 1;

DROP TABLE IF EXISTS t_text_idx_extreme;

CREATE TABLE t_text_idx_extreme
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- Insert 1,000,000 rows with controlled token distribution:
--   dense:     number % 3 = 0     → ~333,333 rows (33.3%)
--   medium:    number % 50 = 0    → ~20,000 rows  (2%)
--   rare:      number % 500 = 0   → ~2,000 rows   (0.2%)
--   ultrarare: number % 5000 = 0  → ~200 rows      (0.02%)
INSERT INTO t_text_idx_extreme
SELECT
    number,
    concat(
        'doc ', toString(number), ' ',
        if(number % 3 = 0, 'dense ', ''),
        if(number % 50 = 0, 'medium ', ''),
        if(number % 500 = 0, 'rare ', ''),
        if(number % 5000 = 0, 'ultrarare ', ''),
        'end'
    )
FROM numbers(1000000);

OPTIMIZE TABLE t_text_idx_extreme FINAL;

-- Query 1: Extreme selectivity difference AND (dense × ultrarare)
--   Expected: number % 3 = 0 AND number % 5000 = 0 → number % 15000 = 0 → 67 rows
SELECT 'Q1 materialize: dense AND ultrarare';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAllTokens(text, ['dense', 'ultrarare'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

SELECT 'Q1 lazy: dense AND ultrarare';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAllTokens(text, ['dense', 'ultrarare'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 2: 4-token sparse AND (dense × medium × rare × ultrarare)
--   Expected: LCM(3,50,500,5000) = 15000 → number % 15000 = 0 → 67 rows
SELECT 'Q2 materialize: 4-token AND';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAllTokens(text, ['dense', 'medium', 'rare', 'ultrarare'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

SELECT 'Q2 lazy: 4-token AND';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAllTokens(text, ['dense', 'medium', 'rare', 'ultrarare'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 3: Medium density AND (dense × medium)
--   Expected: number % 3 = 0 AND number % 50 = 0 → number % 150 = 0 → 6667 rows
SELECT 'Q3 materialize: dense AND medium';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAllTokens(text, ['dense', 'medium'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

SELECT 'Q3 lazy: dense AND medium';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAllTokens(text, ['dense', 'medium'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 4: Sparse OR (rare | ultrarare)
--   Expected: number % 500 = 0 OR number % 5000 = 0 → number % 500 = 0 → 2000 rows
--   (ultrarare is a subset of rare by construction since 5000 % 500 = 0)
SELECT 'Q4 materialize: sparse OR';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAnyTokens(text, ['rare', 'ultrarare'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

SELECT 'Q4 lazy: sparse OR';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAnyTokens(text, ['rare', 'ultrarare'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 5: Mixed density OR (dense | medium | rare)
--   Expected: number % 3 = 0 OR number % 50 = 0 OR number % 500 = 0
--   = rows divisible by 3 + rows divisible by 50 but not 3 + rows divisible by 500 but not 3 and not 50
--   By inclusion-exclusion: |A∪B∪C| = |A| + |B| - |A∩B| (since C ⊂ B)
--   = 333334 + 20000 - 6667 = 346667
SELECT 'Q5 materialize: mixed OR';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAnyTokens(text, ['dense', 'medium', 'rare'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

SELECT 'Q5 lazy: mixed OR';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAnyTokens(text, ['dense', 'medium', 'rare'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 6: Single ultrarare token
--   Expected: number % 5000 = 0 → 200 rows
SELECT 'Q6 materialize: single ultrarare';
SELECT count()
FROM t_text_idx_extreme
WHERE hasToken(text, 'ultrarare')
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

SELECT 'Q6 lazy: single ultrarare';
SELECT count()
FROM t_text_idx_extreme
WHERE hasToken(text, 'ultrarare')
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 7: Double sparse AND (rare × ultrarare)
--   Expected: number % 500 = 0 AND number % 5000 = 0 → number % 5000 = 0 → 200 rows
SELECT 'Q7 materialize: rare AND ultrarare';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAllTokens(text, ['rare', 'ultrarare'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

SELECT 'Q7 lazy: rare AND ultrarare';
SELECT count()
FROM t_text_idx_extreme
WHERE hasAllTokens(text, ['rare', 'ultrarare'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

DROP TABLE t_text_idx_extreme;

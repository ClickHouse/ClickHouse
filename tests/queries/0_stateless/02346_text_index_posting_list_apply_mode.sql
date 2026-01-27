-- This test validates the posting_list_apply_mode parameter and related query settings:
-- 1. posting_list_apply_mode: 'lazy' (stream decode) vs 'materialize' (decode to bitmap first)
-- 2. text_index_brute_force_apply: force brute force algorithm
-- 3. text_index_brute_force_density_threshold: density threshold for auto-switching to brute force
--
-- The 'lazy' mode is only valid when posting_list_codec is not 'none'.

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_lazy;
DROP TABLE IF EXISTS tab_materialize;
DROP TABLE IF EXISTS tab_uncompressed;

-- =============================================================================
-- PART 1: Table Creation and Validation
-- =============================================================================

-- Test 1: Create table with lazy mode (valid: compressed posting list)
CREATE TABLE tab_lazy
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        posting_list_apply_mode = 'lazy'
    )
)
ENGINE = MergeTree
ORDER BY id;

-- Test 2: Create table with materialize mode (valid: compressed posting list)
CREATE TABLE tab_materialize
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        posting_list_apply_mode = 'materialize'
    )
)
ENGINE = MergeTree
ORDER BY id;

-- Test 3: Create table with default mode (materialize) for uncompressed
CREATE TABLE tab_uncompressed
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha'
    )
)
ENGINE = MergeTree
ORDER BY id;

-- Test 4: Verify lazy mode is invalid for uncompressed posting list
CREATE TABLE tab_invalid
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_apply_mode = 'lazy'
    )
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- PART 2: Insert Test Data
-- =============================================================================

-- Insert diverse test data with various token combinations
-- Distribution:
--   number % 10 = 0: 'apple banana orange'      (1000 rows) - 3 tokens
--   number % 10 = 1: 'cherry date'              (1000 rows) - 2 tokens
--   number % 10 = 2: 'apple cherry grape'       (1000 rows) - 3 tokens
--   number % 10 = 3: 'banana date fig'          (1000 rows) - 3 tokens
--   number % 10 = 4: 'elderberry fig'           (1000 rows) - 2 tokens
--   number % 10 = 5: 'apple'                    (1000 rows) - 1 token
--   number % 10 = 6: 'banana cherry'            (1000 rows) - 2 tokens
--   number % 10 = 7: 'grape orange'             (1000 rows) - 2 tokens
--   number % 10 = 8: 'apple banana cherry date' (1000 rows) - 4 tokens
--   number % 10 = 9: 'fig grape orange'         (1000 rows) - 3 tokens

INSERT INTO tab_lazy
SELECT
    number AS id,
    multiIf(
        number % 10 = 0, 'apple banana orange',
        number % 10 = 1, 'cherry date',
        number % 10 = 2, 'apple cherry grape',
        number % 10 = 3, 'banana date fig',
        number % 10 = 4, 'elderberry fig',
        number % 10 = 5, 'apple',
        number % 10 = 6, 'banana cherry',
        number % 10 = 7, 'grape orange',
        number % 10 = 8, 'apple banana cherry date',
        'fig grape orange'
    ) AS str
FROM numbers(10000);

INSERT INTO tab_materialize
SELECT
    number AS id,
    multiIf(
        number % 10 = 0, 'apple banana orange',
        number % 10 = 1, 'cherry date',
        number % 10 = 2, 'apple cherry grape',
        number % 10 = 3, 'banana date fig',
        number % 10 = 4, 'elderberry fig',
        number % 10 = 5, 'apple',
        number % 10 = 6, 'banana cherry',
        number % 10 = 7, 'grape orange',
        number % 10 = 8, 'apple banana cherry date',
        'fig grape orange'
    ) AS str
FROM numbers(10000);

INSERT INTO tab_uncompressed
SELECT
    number AS id,
    multiIf(
        number % 10 = 0, 'apple banana orange',
        number % 10 = 1, 'cherry date',
        number % 10 = 2, 'apple cherry grape',
        number % 10 = 3, 'banana date fig',
        number % 10 = 4, 'elderberry fig',
        number % 10 = 5, 'apple',
        number % 10 = 6, 'banana cherry',
        number % 10 = 7, 'grape orange',
        number % 10 = 8, 'apple banana cherry date',
        'fig grape orange'
    ) AS str
FROM numbers(10000);

OPTIMIZE TABLE tab_lazy FINAL;
OPTIMIZE TABLE tab_materialize FINAL;
OPTIMIZE TABLE tab_uncompressed FINAL;

-- =============================================================================
-- PART 3: hasToken Tests (Single Token Match)
-- =============================================================================

-- Test: hasToken with high-frequency token
-- apple appears in: 0,2,5,8 => 4000 rows
SELECT 'hasToken(apple):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasToken(str, 'apple')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasToken(str, 'apple')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'apple')) AS uncompressed,
    lazy = 4000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasToken with medium-frequency token
-- banana appears in: 0,3,6,8 => 4000 rows
SELECT 'hasToken(banana):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasToken(str, 'banana')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasToken(str, 'banana')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'banana')) AS uncompressed,
    lazy = 4000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasToken with lower-frequency token
-- elderberry appears in: 4 => 1000 rows
SELECT 'hasToken(elderberry):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasToken(str, 'elderberry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasToken(str, 'elderberry')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'elderberry')) AS uncompressed,
    lazy = 1000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasToken with non-existent token
SELECT 'hasToken(nonexistent):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasToken(str, 'nonexistent')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasToken(str, 'nonexistent')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'nonexistent')) AS uncompressed,
    lazy = 0 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasToken with empty token
SELECT 'hasToken(empty):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasToken(str, '')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasToken(str, '')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, '')) AS uncompressed,
    lazy = materialize AND materialize = uncompressed AS ok;

-- =============================================================================
-- PART 4: hasAllTokens Tests (AND / Intersection)
-- =============================================================================

-- Test: hasAllTokens with 2 tokens - common intersection
-- apple AND banana: 0,8 => 2000 rows
SELECT 'hasAllTokens(apple banana):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple banana')) AS uncompressed,
    lazy = 2000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAllTokens with 2 tokens - smaller intersection
-- apple AND cherry: 2,8 => 2000 rows
SELECT 'hasAllTokens(apple cherry):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple cherry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple cherry')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple cherry')) AS uncompressed,
    lazy = 2000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAllTokens with 3 tokens
-- apple AND banana AND orange: 0 => 1000 rows
SELECT 'hasAllTokens(apple banana orange):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana orange')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana orange')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple banana orange')) AS uncompressed,
    lazy = 1000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAllTokens with 4 tokens
-- apple AND banana AND cherry AND date: 8 => 1000 rows
SELECT 'hasAllTokens(apple banana cherry date):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana cherry date')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana cherry date')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple banana cherry date')) AS uncompressed,
    lazy = 1000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAllTokens with empty intersection
-- apple AND elderberry: no row has both => 0 rows
SELECT 'hasAllTokens(apple elderberry):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple elderberry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple elderberry')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple elderberry')) AS uncompressed,
    lazy = 0 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAllTokens with one non-existent token
SELECT 'hasAllTokens(apple nonexistent):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple nonexistent')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple nonexistent')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple nonexistent')) AS uncompressed,
    lazy = 0 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAllTokens with single token (degenerate case)
SELECT 'hasAllTokens(apple) single:';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple')) AS uncompressed,
    lazy = 4000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAllTokens with duplicate tokens
SELECT 'hasAllTokens(apple apple):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple apple')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple apple')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple apple')) AS uncompressed,
    lazy = 4000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- =============================================================================
-- PART 5: hasAnyToken Tests (OR / Union)
-- =============================================================================

-- Test: hasAnyToken with 2 tokens - overlapping sets
-- apple OR cherry: 0,2,5,8 + 1,2,6,8 = 0,1,2,5,6,8 => 6000 rows
SELECT 'hasAnyToken(apple cherry):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAnyToken(str, 'apple cherry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAnyToken(str, 'apple cherry')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAnyToken(str, 'apple cherry')) AS uncompressed,
    lazy = 6000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAnyToken with 2 tokens - disjoint sets
-- apple OR elderberry: 0,2,5,8 + 4 = 0,2,4,5,8 => 5000 rows
SELECT 'hasAnyToken(apple elderberry):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAnyToken(str, 'apple elderberry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAnyToken(str, 'apple elderberry')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAnyToken(str, 'apple elderberry')) AS uncompressed,
    lazy = 5000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAnyToken with 3 tokens
-- apple OR banana OR cherry: 0,2,5,8 + 0,3,6,8 + 1,2,6,8 = 0,1,2,3,5,6,8 => 7000 rows
SELECT 'hasAnyToken(apple banana cherry):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAnyToken(str, 'apple banana cherry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAnyToken(str, 'apple banana cherry')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAnyToken(str, 'apple banana cherry')) AS uncompressed,
    lazy = 7000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAnyToken with 4 tokens
-- apple OR banana OR cherry OR date: covers 0,1,2,3,5,6,8 => 7000 rows (date is in 1,3,8 which are already covered)
SELECT 'hasAnyToken(apple banana cherry date):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAnyToken(str, 'apple banana cherry date')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAnyToken(str, 'apple banana cherry date')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAnyToken(str, 'apple banana cherry date')) AS uncompressed,
    lazy = 7000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAnyToken with all non-existent tokens
SELECT 'hasAnyToken(nonexistent1 nonexistent2):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAnyToken(str, 'nonexistent1 nonexistent2')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAnyToken(str, 'nonexistent1 nonexistent2')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAnyToken(str, 'nonexistent1 nonexistent2')) AS uncompressed,
    lazy = 0 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAnyToken with one existent and one non-existent token
SELECT 'hasAnyToken(apple nonexistent):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAnyToken(str, 'apple nonexistent')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAnyToken(str, 'apple nonexistent')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAnyToken(str, 'apple nonexistent')) AS uncompressed,
    lazy = 4000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAnyToken with single token (degenerate case)
SELECT 'hasAnyToken(apple) single:';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAnyToken(str, 'apple')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAnyToken(str, 'apple')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAnyToken(str, 'apple')) AS uncompressed,
    lazy = 4000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- =============================================================================
-- PART 6: Combined Queries (AND + OR)
-- =============================================================================

-- Test: hasToken AND hasToken
SELECT 'hasToken(apple) AND hasToken(banana):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasToken(str, 'apple') AND hasToken(str, 'banana')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasToken(str, 'apple') AND hasToken(str, 'banana')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'apple') AND hasToken(str, 'banana')) AS uncompressed,
    lazy = 2000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasToken OR hasToken
SELECT 'hasToken(apple) OR hasToken(elderberry):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasToken(str, 'apple') OR hasToken(str, 'elderberry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasToken(str, 'apple') OR hasToken(str, 'elderberry')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'apple') OR hasToken(str, 'elderberry')) AS uncompressed,
    lazy = 5000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAllTokens AND hasToken
-- (apple AND banana) AND cherry: rows with all 3 => 8 => 1000 rows
SELECT 'hasAllTokens(apple banana) AND hasToken(cherry):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana') AND hasToken(str, 'cherry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana') AND hasToken(str, 'cherry')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple banana') AND hasToken(str, 'cherry')) AS uncompressed,
    lazy = 1000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAnyToken AND hasToken
-- (apple OR cherry) AND banana: 0,1,2,5,6,8 intersect 0,3,6,8 = 0,6,8 => 3000 rows
SELECT 'hasAnyToken(apple cherry) AND hasToken(banana):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAnyToken(str, 'apple cherry') AND hasToken(str, 'banana')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAnyToken(str, 'apple cherry') AND hasToken(str, 'banana')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAnyToken(str, 'apple cherry') AND hasToken(str, 'banana')) AS uncompressed,
    lazy = 3000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAllTokens OR hasAllTokens
-- (apple AND banana) OR (cherry AND date): 0,8 + 1,8 = 0,1,8 => 3000 rows
SELECT 'hasAllTokens(apple banana) OR hasAllTokens(cherry date):';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana') OR hasAllTokens(str, 'cherry date')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana') OR hasAllTokens(str, 'cherry date')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple banana') OR hasAllTokens(str, 'cherry date')) AS uncompressed,
    lazy = 3000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: NOT hasToken
SELECT 'NOT hasToken(apple):';
SELECT
    (SELECT count() FROM tab_lazy WHERE NOT hasToken(str, 'apple')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE NOT hasToken(str, 'apple')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE NOT hasToken(str, 'apple')) AS uncompressed,
    lazy = 6000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- =============================================================================
-- PART 7: Brute Force Settings Tests
-- =============================================================================

-- Test: text_index_brute_force_apply = true with hasAllTokens
SELECT 'brute_force_apply=true hasAllTokens:';
SET text_index_brute_force_apply = true;
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana cherry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana cherry')) AS materialize,
    lazy = 1000 AND lazy = materialize AS ok;
SET text_index_brute_force_apply = false;

-- Test: text_index_brute_force_apply = true with hasAnyToken
SELECT 'brute_force_apply=true hasAnyToken:';
SET text_index_brute_force_apply = true;
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAnyToken(str, 'apple elderberry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAnyToken(str, 'apple elderberry')) AS materialize,
    lazy = 5000 AND lazy = materialize AS ok;
SET text_index_brute_force_apply = false;

-- Test: text_index_brute_force_density_threshold = 0.1 (lower threshold)
SELECT 'density_threshold=0.1 hasAllTokens:';
SET text_index_brute_force_density_threshold = 0.1;
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana')) AS materialize,
    lazy = 2000 AND lazy = materialize AS ok;
SET text_index_brute_force_density_threshold = 0.2;

-- Test: text_index_brute_force_density_threshold = 0.5 (higher threshold)
SELECT 'density_threshold=0.5 hasAllTokens:';
SET text_index_brute_force_density_threshold = 0.5;
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana cherry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana cherry')) AS materialize,
    lazy = 1000 AND lazy = materialize AS ok;
SET text_index_brute_force_density_threshold = 0.2;

-- Test: text_index_brute_force_density_threshold = 0.0 (always brute force when density > 0)
SELECT 'density_threshold=0.0 hasAllTokens:';
SET text_index_brute_force_density_threshold = 0.0;
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana')) AS materialize,
    lazy = 2000 AND lazy = materialize AS ok;
SET text_index_brute_force_density_threshold = 0.2;

-- Test: text_index_brute_force_density_threshold = 1.0 (never brute force, density < 1.0)
SELECT 'density_threshold=1.0 hasAllTokens:';
SET text_index_brute_force_density_threshold = 1.0;
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana')) AS materialize,
    lazy = 2000 AND lazy = materialize AS ok;
SET text_index_brute_force_density_threshold = 0.2;

-- =============================================================================
-- PART 8: Edge Cases
-- =============================================================================

-- Test: Query with many tokens (5 tokens)
SELECT 'hasAllTokens 5 tokens:';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAllTokens(str, 'apple banana cherry date orange')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAllTokens(str, 'apple banana cherry date orange')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAllTokens(str, 'apple banana cherry date orange')) AS uncompressed,
    lazy = 0 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: hasAnyToken with many tokens (5 tokens)
-- apple OR banana OR cherry OR date OR elderberry: 0,1,2,3,4,5,6,8 => 8000 rows
SELECT 'hasAnyToken 5 tokens:';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasAnyToken(str, 'apple banana cherry date elderberry')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasAnyToken(str, 'apple banana cherry date elderberry')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasAnyToken(str, 'apple banana cherry date elderberry')) AS uncompressed,
    lazy = 8000 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: Case sensitivity (tokens should be case-sensitive by default)
SELECT 'hasToken case sensitivity:';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasToken(str, 'Apple')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasToken(str, 'Apple')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'Apple')) AS uncompressed,
    lazy = 0 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: Partial token match (should not match)
SELECT 'hasToken partial match:';
SELECT
    (SELECT count() FROM tab_lazy WHERE hasToken(str, 'app')) AS lazy,
    (SELECT count() FROM tab_materialize WHERE hasToken(str, 'app')) AS materialize,
    (SELECT count() FROM tab_uncompressed WHERE hasToken(str, 'app')) AS uncompressed,
    lazy = 0 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: Query with LIMIT
SELECT 'hasToken with LIMIT:';
SELECT
    (SELECT count() FROM (SELECT * FROM tab_lazy WHERE hasToken(str, 'apple') LIMIT 100)) AS lazy,
    (SELECT count() FROM (SELECT * FROM tab_materialize WHERE hasToken(str, 'apple') LIMIT 100)) AS materialize,
    (SELECT count() FROM (SELECT * FROM tab_uncompressed WHERE hasToken(str, 'apple') LIMIT 100)) AS uncompressed,
    lazy = 100 AND lazy = materialize AND materialize = uncompressed AS ok;

-- Test: Query with ORDER BY
SELECT 'hasToken with ORDER BY:';
SELECT
    (SELECT max(id) FROM tab_lazy WHERE hasToken(str, 'apple') GROUP BY id ORDER BY id DESC LIMIT 1) AS lazy,
    (SELECT max(id) FROM tab_materialize WHERE hasToken(str, 'apple') GROUP BY id ORDER BY id DESC LIMIT 1) AS materialize,
    (SELECT max(id) FROM tab_uncompressed WHERE hasToken(str, 'apple') GROUP BY id ORDER BY id DESC LIMIT 1) AS uncompressed,
    lazy = materialize AND materialize = uncompressed AS ok;

-- =============================================================================
-- PART 9: Stress Tests with Larger Data
-- =============================================================================

DROP TABLE IF EXISTS tab_stress;
CREATE TABLE tab_stress
(
    id UInt64,
    str String,
    INDEX inv_idx str TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        posting_list_apply_mode = 'lazy'
    )
)
ENGINE = MergeTree
ORDER BY id;

-- Insert 100K rows
INSERT INTO tab_stress
SELECT
    number AS id,
    concat(
        if(number % 2 = 0, 'alpha ', ''),
        if(number % 3 = 0, 'beta ', ''),
        if(number % 5 = 0, 'gamma ', ''),
        if(number % 7 = 0, 'delta ', ''),
        if(number % 11 = 0, 'epsilon ', ''),
        'base'
    ) AS str
FROM numbers(100000);

OPTIMIZE TABLE tab_stress FINAL;

-- Test: Stress hasToken
SELECT 'Stress hasToken(alpha):';
SELECT
    (SELECT count() FROM tab_stress WHERE hasToken(str, 'alpha')) AS cnt,
    cnt = 50000 AS ok;

-- Test: Stress hasAllTokens (2 tokens)
-- alpha AND beta: divisible by 2 AND 3 => divisible by 6 => 100000/6 = 16666
SELECT 'Stress hasAllTokens(alpha beta):';
SELECT
    (SELECT count() FROM tab_stress WHERE hasAllTokens(str, 'alpha beta')) AS cnt,
    cnt = 16667 AS ok;

-- Test: Stress hasAllTokens (3 tokens)
-- alpha AND beta AND gamma: divisible by 2,3,5 => divisible by 30 => 100000/30 = 3333
SELECT 'Stress hasAllTokens(alpha beta gamma):';
SELECT
    (SELECT count() FROM tab_stress WHERE hasAllTokens(str, 'alpha beta gamma')) AS cnt,
    cnt = 3334 AS ok;

-- Test: Stress hasAllTokens (4 tokens)
-- alpha AND beta AND gamma AND delta: divisible by 2,3,5,7 => divisible by 210 => 100000/210 = 476
SELECT 'Stress hasAllTokens(alpha beta gamma delta):';
SELECT
    (SELECT count() FROM tab_stress WHERE hasAllTokens(str, 'alpha beta gamma delta')) AS cnt,
    cnt = 477 AS ok;

-- Test: Stress hasAnyToken (3 tokens)
-- alpha OR beta OR gamma: 50000 + 33333 + 20000 - overlaps
SELECT 'Stress hasAnyToken(alpha beta gamma):';
SELECT
    (SELECT count() FROM tab_stress WHERE hasAnyToken(str, 'alpha beta gamma')) AS cnt,
    cnt > 60000 AS ok;

-- Test: Stress with brute force
SELECT 'Stress brute_force hasAllTokens:';
SET text_index_brute_force_apply = true;
SELECT
    (SELECT count() FROM tab_stress WHERE hasAllTokens(str, 'alpha beta gamma')) AS cnt,
    cnt = 3334 AS ok;
SET text_index_brute_force_apply = false;

DROP TABLE tab_stress;

-- =============================================================================
-- Cleanup
-- =============================================================================

DROP TABLE tab_lazy;
DROP TABLE tab_materialize;
DROP TABLE tab_uncompressed;

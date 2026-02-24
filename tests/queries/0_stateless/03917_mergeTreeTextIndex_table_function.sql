-- Tags: no-fasttest
-- Testing the mergeTreeTextIndex table function

DROP TABLE IF EXISTS text_idx_tf;

SET enable_full_text_index = 1;

-- Create a table with a text index using small dictionary_block_size
-- to produce multiple dictionary blocks for testing KeyCondition filtering.
CREATE TABLE text_idx_tf
(
    id UInt64,
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 8)
)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES text_idx_tf;

-- Insert two separate parts so we can test part_name filtering.
INSERT INTO text_idx_tf SELECT number, concatWithSeparator(' ', 'apple', 'banana') FROM numbers(500);
INSERT INTO text_idx_tf SELECT 500 + number, concatWithSeparator(' ', 'cherry', 'date') FROM numbers(500);

SELECT '-- 1. All selected fields';
SELECT * FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
ORDER BY part_name, token;

SELECT '-- 2. Aggregation over token: count distinct tokens per part';
SELECT part_name, count() AS token_count
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
GROUP BY part_name
ORDER BY part_name;

SELECT '-- 3. Aggregation: total cardinality per token across all parts';
SELECT token, sum(cardinality) AS total_cardinality
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
GROUP BY token
ORDER BY token;

SELECT '-- 4. Filtering by part_name: only read tokens from one part';
SELECT token FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE part_name = 'all_1_1_0'
ORDER BY token;

SELECT '-- 5. KeyCondition filtering: equality on token';
SELECT token, cardinality
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token = 'apple';

SELECT '-- 6. KeyCondition filtering: range on token';
SELECT token, cardinality
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token >= 'b' AND token < 'c'
ORDER BY part_name, token;

SELECT '-- 7. KeyCondition filtering: token IN (...)';
SELECT token, cardinality
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token IN ('apple', 'cherry')
ORDER BY part_name, token;

SELECT '-- 8. KeyCondition filtering: prefix match with LIKE';
SELECT token, cardinality
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token LIKE 'da%'
ORDER BY part_name, token;

DROP TABLE text_idx_tf;

-- ==========================================
-- Test with many dictionary blocks and max_rows_to_read validation.
-- dictionary_block_size = 4 means each block holds at most 4 tokens.
-- Using single-letter tokens (a..z) to get clean block boundaries.
-- Sorted blocks: [a,b,c,d] [e,f,g,h] [i,j,k,l] [m,n,o,p] [q,r,s,t] [u,v,w,x] [y,z]
-- ==========================================

CREATE TABLE text_idx_tf
(
    id UInt64,
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4)
)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES text_idx_tf;

-- Insert 26 rows, each with a single-letter token.
INSERT INTO text_idx_tf SELECT number, char(97 + number) FROM numbers(26);

SELECT '-- 9. KeyCondition filters to a single block out of 7';
SELECT count()
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token = 'a'
SETTINGS max_rows_to_read = 4;

SELECT '-- 10. Range filter matching exactly one block';
SELECT token
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token >= 'e' AND token <= 'h'
ORDER BY token
SETTINGS max_rows_to_read = 4;

SELECT '-- 11. Range filter matching two blocks';
SELECT count()
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token >= 'a' AND token < 'i'
SETTINGS max_rows_to_read = 8;

DROP TABLE text_idx_tf;

-- ==========================================
-- Test with a larger table (100000 rows) and posting_list_block_size
-- ==========================================

CREATE TABLE text_idx_tf
(
    id UInt64,
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 8, posting_list_block_size = 1024)
)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES text_idx_tf;

-- Insert 100000 rows with several token patterns.
-- Tokens: 'common' appears in every row, 'rare' in 1 row,
-- 'medium' in every 100th row, plus a number-based token.
INSERT INTO text_idx_tf
SELECT
    number,
    concatWithSeparator(' ',
        'common',
        if(number = 42, 'rare', ''),
        if(number % 100 = 0, 'medium', ''),
        concat('tok', toString(number % 10000)))
FROM numbers(100000);

SELECT '-- 12. Large table: count tokens';
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s);

SELECT '-- 13. Large table: uniqExact tokens';
SELECT uniqExact(token) FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s);

SELECT '-- 14. Large table: aggregation over token (top tokens by cardinality)';
SELECT token, cardinality, num_posting_blocks
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token IN ('common', 'rare', 'medium')
ORDER BY token
SETTINGS max_rows_to_read = 8;

SELECT '-- 15. Large table: filter on equality';
SELECT count()
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token = 'common'
SETTINGS max_rows_to_read = 8;

SELECT '-- 16. Large table: range filter on token';
SELECT count()
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token >= 'tok0' AND token <= 'tok1'
SETTINGS max_rows_to_read = 8;

SELECT '-- 17. Large table: LIKE filter on token';
SELECT count(), sum(cardinality)
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE token LIKE 'med%'
SETTINGS max_rows_to_read = 8;

DROP TABLE text_idx_tf;

-- ==========================================
-- Test with multiple parts and combined part_name + token filters
-- ==========================================

CREATE TABLE text_idx_tf
(
    id UInt64,
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 1)
)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES text_idx_tf;

-- Three separate inserts = three parts.
INSERT INTO text_idx_tf SELECT number, 'alpha beta gamma' FROM numbers(100);
INSERT INTO text_idx_tf SELECT 100 + number, 'delta epsilon zeta' FROM numbers(100);
INSERT INTO text_idx_tf SELECT 200 + number, 'eta theta iota' FROM numbers(100);

SELECT '-- 18. All tokens from all parts';
SELECT part_name, token
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
ORDER BY part_name, token;

SELECT '-- 19. Combined part_name and token filter';
SELECT token, cardinality
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE part_name = 'all_1_1_0' AND token = 'alpha'
ORDER BY token
SETTINGS max_rows_to_read = 1;

SELECT '-- 20. All tokens from one specific part';
SELECT token
FROM mergeTreeTextIndex(currentDatabase(), text_idx_tf, idx_s)
WHERE part_name = 'all_1_1_0'
ORDER BY token
SETTINGS max_rows_to_read = 3;

DROP TABLE text_idx_tf;

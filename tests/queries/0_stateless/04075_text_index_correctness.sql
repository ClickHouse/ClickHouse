-- Equivalence test: lazy and materialize modes must produce identical query results.
-- Each query is run with both modes; EXCEPT verifies the result sets are identical.
-- If any EXCEPT returns rows, the modes disagree — that's a bug.

SET enable_full_text_index = 1;
SET allow_experimental_text_index_lazy_apply = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

DROP TABLE IF EXISTS tab_eq;

CREATE TABLE tab_eq(
    k UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128, posting_list_codec = 'bitpacking')
) ENGINE = MergeTree() ORDER BY k
  SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

----------------------------------------------------
SELECT 'Part 1: Small data (300 rows)';
----------------------------------------------------

INSERT INTO tab_eq SELECT number, if(number % 3 = 0, 'apple banana', if(number % 3 = 1, 'cherry date', 'elderberry fig')) FROM numbers(300);

-- hasToken: single tokens
SELECT 'hasToken apple — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasToken(s, 'apple') SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasToken(s, 'apple') SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

SELECT 'hasToken cherry — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasToken(s, 'cherry') SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasToken(s, 'cherry') SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

-- hasToken: AND (intersection)
SELECT 'AND apple+banana — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasToken(s, 'apple') AND hasToken(s, 'banana') SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasToken(s, 'apple') AND hasToken(s, 'banana') SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

-- hasToken: OR (union)
SELECT 'OR apple|cherry — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasToken(s, 'apple') OR hasToken(s, 'cherry') SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasToken(s, 'apple') OR hasToken(s, 'cherry') SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

-- hasAnyTokens
SELECT 'hasAnyTokens [apple,cherry] — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasAnyTokens(s, ['apple', 'cherry']) SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasAnyTokens(s, ['apple', 'cherry']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

-- hasAllTokens
SELECT 'hasAllTokens [apple,banana] — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasAllTokens(s, ['apple', 'banana']) SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasAllTokens(s, ['apple', 'banana']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

-- Non-existent token
SELECT 'hasToken nonexistent — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasToken(s, 'grape') SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasToken(s, 'grape') SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

----------------------------------------------------
SELECT 'Part 2: Large data (2000 rows, multi-segment)';
----------------------------------------------------

TRUNCATE TABLE tab_eq;

INSERT INTO tab_eq SELECT number,
    concat(
        if(number % 3 = 0, 'red ', ''),
        if(number % 5 = 0, 'blue ', ''),
        if(number % 7 = 0, 'green ', ''),
        'base'
    )
FROM numbers(2000);

SELECT 'hasToken red — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasToken(s, 'red') SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasToken(s, 'red') SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

SELECT 'AND red+blue — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasAllTokens(s, ['red', 'blue']) SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasAllTokens(s, ['red', 'blue']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

SELECT 'AND red+blue+green — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasAllTokens(s, ['red', 'blue', 'green']) SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasAllTokens(s, ['red', 'blue', 'green']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

SELECT 'OR red|blue|green — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasAnyTokens(s, ['red', 'blue', 'green']) SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasAnyTokens(s, ['red', 'blue', 'green']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

----------------------------------------------------
SELECT 'Part 3: After merge (1000+1000 rows)';
----------------------------------------------------

TRUNCATE TABLE tab_eq;

INSERT INTO tab_eq SELECT number, concat(if(number % 4 = 0, 'hot ', ''), if(number % 6 = 0, 'cold ', ''), 'item') FROM numbers(1000);
INSERT INTO tab_eq SELECT number + 1000, concat(if((number+1000) % 4 = 0, 'hot ', ''), if((number+1000) % 6 = 0, 'cold ', ''), 'item') FROM numbers(1000);

OPTIMIZE TABLE tab_eq FINAL;

SELECT 'hasToken hot (merged) — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasToken(s, 'hot') SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasToken(s, 'hot') SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

SELECT 'AND hot+cold (merged) — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasAllTokens(s, ['hot', 'cold']) SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasAllTokens(s, ['hot', 'cold']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

SELECT 'OR hot|cold (merged) — diff count:';
SELECT count() FROM (
    (SELECT k FROM tab_eq WHERE hasAnyTokens(s, ['hot', 'cold']) SETTINGS text_index_posting_list_apply_mode = 'lazy')
    EXCEPT
    (SELECT k FROM tab_eq WHERE hasAnyTokens(s, ['hot', 'cold']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
);

----------------------------------------------------
DROP TABLE IF EXISTS tab_eq;

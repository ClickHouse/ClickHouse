SET allow_experimental_full_text_index = 1;
SET enable_lightweight_update = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_lwu_text_index;

CREATE TABLE t_lwu_text_index
(
    str String,
    val UInt64,
    INDEX idx_str (str) TYPE text(tokenizer = splitByNonAlpha()) GRANULARITY 8
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_text_index SELECT toString(number), number * 2 FROM numbers(100000);

-- Baseline: hasToken(str, '777') matches exactly 1 row (str='777')
SELECT count() FROM t_lwu_text_index WHERE hasToken(str, '777');

-- Update val only (str unchanged): text index stays valid
UPDATE t_lwu_text_index SET val = val + 1 WHERE hasToken(str, '777');

SELECT count() FROM t_lwu_text_index WHERE hasToken(str, '777');

-- Update str to ADD '777' token to rows in a different text index mark.
-- Rows 70000-70009 are in text index mark 1 (>= 65536), separate from row 777 (mark 0).
-- On master without the fix, the text index direct read optimization uses stale index data
-- and misses the updated rows because their mark has no '777' token in the index.
UPDATE t_lwu_text_index SET str = '777' WHERE val >= 140000 AND val < 140020;

SELECT count() FROM t_lwu_text_index WHERE hasToken(str, '777');

DROP TABLE t_lwu_text_index;

-- A patch part on the indexed column disables the direct read, but the preprocessor must stay applied: otherwise
-- the predicate silently falls back to raw semantics and stops matching until the patch is merged away.
DROP TABLE IF EXISTS t_lwu_text_index_pp;

CREATE TABLE t_lwu_text_index_pp
(
    id UInt64,
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = splitByNonAlpha(), preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_text_index_pp VALUES (1, 'Beta'), (2, 'Gamma');

SELECT count() FROM t_lwu_text_index_pp WHERE hasToken(s, 'beta') SETTINGS use_skip_indexes = 1;

-- A no-op update of the indexed column on an unrelated row must not change the answer.
UPDATE t_lwu_text_index_pp SET s = s WHERE id = 2;

SELECT count() FROM t_lwu_text_index_pp WHERE hasToken(s, 'beta') SETTINGS use_skip_indexes = 1;

-- Updates that really change the data still give the up-to-date answer.
UPDATE t_lwu_text_index_pp SET s = 'Delta' WHERE id = 1;

SELECT count() FROM t_lwu_text_index_pp WHERE hasToken(s, 'beta') SETTINGS use_skip_indexes = 1;

UPDATE t_lwu_text_index_pp SET s = 'Beta' WHERE id = 2;

SELECT count() FROM t_lwu_text_index_pp WHERE hasToken(s, 'beta') SETTINGS use_skip_indexes = 1;

DROP TABLE t_lwu_text_index_pp;

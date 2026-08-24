-- Tests that LIKE evaluation via dictionary scan returns correct results when
-- `text_index_posting_list_apply_mode = 'lazy'` is requested.
--
-- Regression test for the case where lazy mode skipped `readPostingsIfNeeded` for
-- the whole mark, but `fillColumn`'s lazy path only covered pure-token queries —
-- pattern queries still took the eager `applyPostingsAny` branch against an empty
-- `PostingsMap`, silently filtering out matching rows.
-- See https://github.com/ClickHouse/ClickHouse/pull/100035#discussion_r3267067617

SET enable_full_text_index = 1;
SET allow_experimental_text_index_lazy_apply = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_posting_list_apply_mode = 'lazy';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 128)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- Enough rows to exceed the rare-token threshold so postings go through the
-- packed-block path that lazy mode targets.
INSERT INTO tab SELECT number,
    concat(
        if(number % 2 = 0, 'foo', 'qux'), ' ',
        if(number % 3 = 0, 'bar', 'baz'), ' ',
        'common'
    )
FROM numbers(2000);

SELECT 'Pure LIKE patterns under lazy mode (must not return false negatives)';

-- 'foo' appears in every even row -> 1000 rows
SELECT count() FROM tab WHERE message LIKE '%foo%';
-- 'bar' appears every 3rd row -> 667 rows (rows 0,3,6,...,1998)
SELECT count() FROM tab WHERE message LIKE '%bar%';
-- 'common' appears in every row -> 2000 rows
SELECT count() FROM tab WHERE message LIKE '%common%';
-- non-existent
SELECT count() FROM tab WHERE message LIKE '%nonexistent%';

SELECT 'Multiple LIKE predicates AND/OR under lazy mode';

-- foo AND bar -> rows with number % 6 == 0 -> 334 rows (0,6,...,1998)
SELECT count() FROM tab WHERE message LIKE '%foo%' AND message LIKE '%bar%';
-- foo OR bar -> 1000 + 667 - 334 = 1333 rows
SELECT count() FROM tab WHERE message LIKE '%foo%' OR message LIKE '%bar%';

SELECT 'Mixed token + LIKE predicates under lazy mode';

-- hasToken (would take lazy path on its own) combined with LIKE pattern in the
-- same WHERE clause: both virtual columns belong to one reader, so the patterns
-- column must force eager posting materialization for the reader.
SELECT count() FROM tab WHERE hasToken(message, 'foo') AND message LIKE '%bar%';
SELECT count() FROM tab WHERE hasToken(message, 'common') AND message LIKE '%foo%';
SELECT count() FROM tab WHERE hasAnyTokens(message, ['foo', 'qux']) AND message LIKE '%common%';

SELECT 'NOT LIKE under lazy mode';

-- NOT LIKE %foo% -> odd rows = 1000
SELECT count() FROM tab WHERE message NOT LIKE '%foo%';
-- NOT LIKE %nonexistent% -> all rows = 2000
SELECT count() FROM tab WHERE message NOT LIKE '%nonexistent%';

SELECT 'Lazy mode still active for pure-token queries on the same table';

-- Pure-token queries on this table (no patterns in the reader) should still take
-- the lazy cursor path. Result must match the non-lazy baseline.
SELECT count() FROM tab WHERE hasToken(message, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(message, ['foo', 'common']);

DROP TABLE tab;

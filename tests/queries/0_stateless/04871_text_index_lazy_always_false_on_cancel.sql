-- Tags: no-parallel
-- no-parallel: enables a global failpoint.

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/114603.
-- When a query is cancelled while a read task is building the skip-index read result,
-- `MergeTreeSkipIndexReader::read` returns a null result and the text index reader analyzes the
-- granule itself. A token missing from the index fails one `hasAllTokens` conjunct, `always_false`
-- is set and `analyzePostings` skips reading posting lists of the still-live conjunct. The lazy
-- apply mode then tried to create a posting-list cursor over the unread raw postings of 'zrare'
-- and threw a logical error "Multi-block postings must be compressed".

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SET use_query_condition_cache = 0;
SET use_text_index_tokens_cache = 1;
-- The minmax-count projection estimation evaluates skip indexes at planning time and can answer
-- `count()` without any read, and the reading below must happen for the cancellation to hit it.
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS tab_lazy_always_false;

-- 'zrare' has cardinality 7: above the embedded-postings threshold (6), below the raw-postings
-- threshold (12), so its posting list is stored raw (uncompressed) and cannot back a lazy cursor.
-- Its row range [1, 997] covers the 'filler' row range [13, 995], so the coarse rows-range clip
-- cannot fail the live query at the dictionary stage; the posting lists are disjoint, but that is
-- only discovered in `analyzePostings`. A compressing codec is required for the lazy apply mode.
CREATE TABLE tab_lazy_always_false
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO tab_lazy_always_false
SELECT number, if(number % 2 = 0, 'common1 common2', if(number < 12 OR number = 997, 'zrare', 'filler'))
FROM numbers(1000);

-- Warm the tokens cache with positive entries for 'filler' and 'zrare'. Without them the reader's
-- own dictionary analysis stops on the missing token before the live query gets its token infos.
SELECT count() FROM tab_lazy_always_false WHERE hasAllTokens(s, ['filler', 'zrare']);

-- Make the skip-index read result build slow enough for the cancellation (the exception from the
-- second UNION branch) to reliably land inside it; the cancelled build returns a null result and
-- the text index reader analyzes the granule itself instead of using a precomputed one.
SYSTEM ENABLE FAILPOINT slowdown_skip_index_read_result_build;

SELECT * FROM
(
    -- HAVING keeps the count row out of the output: on a slow or degraded run the branch may
    -- finish before the exception from the second branch, and a streamed row would otherwise
    -- make the output depend on that timing.
    SELECT count() FROM tab_lazy_always_false
    PREWHERE hasAllTokens(s, ['filler', 'zrare'])
    WHERE hasAllTokens(s, ['zzzabsent', 'filler'])
    HAVING count() < 0
    UNION ALL
    SELECT throwIf(sleepEachRow(0.05) >= 0, 'boom') FROM numbers(1)
); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SYSTEM DISABLE FAILPOINT slowdown_skip_index_read_result_build;

DROP TABLE tab_lazy_always_false;

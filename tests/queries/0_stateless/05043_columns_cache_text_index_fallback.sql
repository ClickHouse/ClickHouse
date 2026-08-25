-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-parallel-replicas
-- A direct-read text-index query in hint mode must re-check the predicate on the
-- physical column through the fallback reader of `MergeTreeReaderTextIndex`. The
-- fallback columns are discovered after the read pool sized the query's columns
-- cache write estimate, so the fallback reader must not write them to the cache.
-- This test pins that contract: with `use_columns_cache = 1`, the fallback read
-- of `s` leaves no entries for `s` in `system.columns_cache`, while an ordinary
-- read of `id` in the same environment does populate the cache.

SET enable_analyzer = 1;
SET max_threads = 1;

-- Force the direct read from the text index; CI may inject these as false, in
-- which case the query would just scan `s` and never reach the fallback reader.
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

-- Disable the dictionary-scan `LIKE` evaluation so the `LIKE` below cannot be
-- answered exactly from the index and takes the hint path with the fallback
-- to physical-column evaluation.
SET use_text_index_like_evaluation_by_dictionary_scan = 0;

DROP TABLE IF EXISTS t_cache_text_fallback;

CREATE TABLE t_cache_text_fallback
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_text_fallback SELECT number, concat('token', toString(number % 100), ' payload') FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- An ordinary read that populates the cache for `id`, proving the cache is
-- active in this environment (the assertion on `s` below is not vacuous).
SELECT sum(id) FROM t_cache_text_fallback SETTINGS use_columns_cache = 1;

-- The direct-read text-index query: the index only hints, so `s` is read
-- physically by the fallback reader with cache writes disabled.
SELECT count() FROM t_cache_text_fallback WHERE s LIKE '%token42 pa%'
SETTINGS use_columns_cache = 1, log_comment = 'columns_cache_text_index_fallback_hint';

-- The same query without the index must return the same count.
SELECT count() FROM t_cache_text_fallback WHERE s LIKE '%token42 pa%'
SETTINGS use_skip_indexes = 0, use_columns_cache = 0;

-- The cache holds entries for `id` (ordinary reader) and none for `s`
-- (fallback reader never writes).
SELECT countIf(column = 'id') > 0, countIf(column = 's')
FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cache_text_fallback';

SYSTEM FLUSH LOGS query_log;

-- Assert the indexed query really took the hint path (fallback exercised):
-- if the hint were discarded or direct read skipped, this test would not be
-- testing anything.
SELECT ProfileEvents['TextIndexUseHint'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
    AND log_comment = 'columns_cache_text_index_fallback_hint';

DROP TABLE t_cache_text_fallback;

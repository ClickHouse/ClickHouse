-- A `hasAllTokens` that cannot match in the part must stay false in lazy apply mode
-- when another search in the same query uses one of its tokens.

DROP TABLE IF EXISTS tab_failed_search;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_skip_indexes_for_disjunctions = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_count_from_text_index = 0;
SET use_query_condition_cache = 0;
SET text_index_posting_list_apply_mode = 'lazy';

DROP TABLE IF EXISTS tab_failed_search;

CREATE TABLE tab_failed_search
(
    k UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 128)
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';

-- 'alpha' is in rows [0, 500) and 'beta' in rows [900, 1000), so `hasAllTokens(s, ['alpha', 'beta'])` can never match.
INSERT INTO tab_failed_search
SELECT number, concat('w', if(number < 500, ' alpha', ''), if(number >= 900, ' beta', ''), if(number % 10 = 0, ' gamma', ''))
FROM numbers(1000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

-- 'alpha' must span several compressed posting blocks, so that it is left to the lazy cursors.
SELECT token, num_posting_blocks > 1, has_compressed_postings FROM mergeTreeTextIndex(currentDatabase(), tab_failed_search, idx) WHERE token = 'alpha';

SELECT 'or, no index', count(), sum(k) FROM tab_failed_search WHERE hasAllTokens(s, ['alpha', 'beta']) OR hasAllTokens(s, ['alpha', 'gamma'])
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'or, lazy', count(), sum(k) FROM tab_failed_search WHERE hasAllTokens(s, ['alpha', 'beta']) OR hasAllTokens(s, ['alpha', 'gamma'])
SETTINGS log_comment = '05331_or_lazy';

SELECT 'not, no index', count(), sum(k) FROM tab_failed_search WHERE hasToken(s, 'alpha') AND NOT hasAllTokens(s, ['alpha', 'beta'])
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'not, lazy', count(), sum(k) FROM tab_failed_search WHERE hasToken(s, 'alpha') AND NOT hasAllTokens(s, ['alpha', 'beta'])
SETTINGS log_comment = '05331_not_lazy';

-- Both searches of each lazy query must be read from the index as `__text_index_*` columns, or the lazy reader is
-- never reached. Parallel replicas are disabled because without a local plan EXPLAIN shows only the remote read.
-- Unused-column pruning is pinned because CI randomizes it.
SELECT 'or, index columns', uniqExactArray(extractAll(explain, '__text_index_\\w+'))
FROM (EXPLAIN actions = 1 SELECT count(), sum(k) FROM tab_failed_search WHERE hasAllTokens(s, ['alpha', 'beta']) OR hasAllTokens(s, ['alpha', 'gamma']) SETTINGS enable_parallel_replicas = 0, query_plan_remove_unused_columns = 1);
SELECT 'not, index columns', uniqExactArray(extractAll(explain, '__text_index_\\w+'))
FROM (EXPLAIN actions = 1 SELECT count(), sum(k) FROM tab_failed_search WHERE hasToken(s, 'alpha') AND NOT hasAllTokens(s, ['alpha', 'beta']) SETTINGS enable_parallel_replicas = 0, query_plan_remove_unused_columns = 1);

-- The materialize mode reads the same columns, so check that both lazy queries iterated lazy cursors.
-- Under parallel replicas without a local plan the counters land on the replicas' rows, so sum over every row of each query.
-- A retry can reuse the database, so only queries since this run created the table count.
SYSTEM FLUSH LOGS query_log;
WITH (SELECT metadata_modification_time FROM system.tables WHERE database = currentDatabase() AND name = 'tab_failed_search') AS run_start
SELECT log_comment, sum(ProfileEvents['TextIndexLazySegmentsPrepared']) > 0
FROM system.query_log
WHERE event_date >= toDate(run_start) AND event_time >= run_start AND type = 'QueryFinish'
  AND initial_query_id IN
  (
      SELECT query_id FROM system.query_log
      WHERE event_date >= toDate(run_start) AND event_time >= run_start AND type = 'QueryFinish'
        AND current_database = currentDatabase() AND is_initial_query AND log_comment IN ('05331_or_lazy', '05331_not_lazy')
  )
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE tab_failed_search;

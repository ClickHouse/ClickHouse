-- Tags: no-parallel-replicas
-- `ReadFromTextIndexCount` reports the rows of the selected ranges of the answered parts as read rows, not one row per part.

SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;
-- The granules pruned by the query condition cache entries of the previous queries disable the optimization.
SET use_query_condition_cache = 0;
-- `max_rows_to_read` with `read_overflow_mode = 'throw'` disables `use_skip_indexes_on_data_read`.
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, if(number % 2 = 0, 'alpha beta', 'gamma delta') FROM numbers(1000);
INSERT INTO tab SELECT number, if(number % 4 = 0, 'alpha epsilon', 'zeta') FROM numbers(3000);

-- The index prunes the granules without the token inside a part.
DROP TABLE IF EXISTS tab_ranges;

CREATE TABLE tab_ranges
(
    id UInt64,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';

INSERT INTO tab_ranges SELECT number, if(number < 100, 'needle', 'hay') FROM numbers(1000);

-- The parts and the granules without the needle tokens are pruned at plan time: only the rows of the selected ranges
-- are reported, the same as without the optimization.
SET use_skip_indexes_on_data_read = 0;

SELECT count() FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha')) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS log_comment = '05371_single_token';
-- The second part has no `beta`, so it is pruned and not processed.
SELECT count() FROM tab WHERE hasAllTokens(text, ['alpha', 'beta']) SETTINGS log_comment = '05371_all_tokens';
SELECT count() FROM tab WHERE hasAnyTokens(text, ['beta', 'zeta']) SETTINGS log_comment = '05371_any_tokens';

SELECT count() FROM (EXPLAIN SELECT count() FROM tab_ranges WHERE hasToken(text, 'needle')) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab_ranges WHERE hasToken(text, 'needle') SETTINGS log_comment = '05371_selected_ranges';
SELECT count() FROM tab_ranges WHERE hasToken(text, 'needle') SETTINGS query_plan_optimize_count_from_text_index = 0, log_comment = '05371_selected_ranges_off';

-- The index is not applied at plan time, the unoptimized read skips the granules without the needle tokens on data read.
-- The count from the text index reads no data, so it reports all rows of the parts, more than the unoptimized read.
SET use_skip_indexes_on_data_read = 1;

SELECT count() FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha')) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS log_comment = '05371_single_token_on_read';
SELECT count() FROM tab WHERE hasAllTokens(text, ['alpha', 'beta']) SETTINGS log_comment = '05371_all_tokens_on_read';
SELECT count() FROM tab WHERE hasAnyTokens(text, ['beta', 'zeta']) SETTINGS log_comment = '05371_any_tokens_on_read';

SELECT count() FROM (EXPLAIN SELECT count() FROM tab_ranges WHERE hasToken(text, 'needle')) WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() FROM tab_ranges WHERE hasToken(text, 'needle') SETTINGS log_comment = '05371_selected_ranges_on_read';
SELECT count() FROM tab_ranges WHERE hasToken(text, 'needle') SETTINGS query_plan_optimize_count_from_text_index = 0, log_comment = '05371_selected_ranges_on_read_off';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, read_rows, read_bytes
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN (
    '05371_single_token', '05371_all_tokens', '05371_any_tokens', '05371_selected_ranges',
    '05371_single_token_on_read', '05371_all_tokens_on_read', '05371_any_tokens_on_read', '05371_selected_ranges_on_read')
ORDER BY event_time_microseconds;

SELECT log_comment, read_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05371_selected_ranges_off', '05371_selected_ranges_on_read_off')
ORDER BY event_time_microseconds;

DROP TABLE tab;
DROP TABLE tab_ranges;

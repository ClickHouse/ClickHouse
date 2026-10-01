-- Tags: no-parallel-replicas
-- `ReadFromTextIndexCount` reports the rows of the answered parts as read rows, not one row per part.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;
-- The parts without the needle tokens are pruned at plan time, not on data read.
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, if(number % 2 = 0, 'alpha beta', 'gamma delta') FROM numbers(1000);
INSERT INTO tab SELECT number, if(number % 4 = 0, 'alpha epsilon', 'zeta') FROM numbers(3000);

SELECT count() FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(text, 'alpha')) WHERE explain LIKE '%ReadFromTextIndexCount%';

SELECT count() FROM tab WHERE hasToken(text, 'alpha') SETTINGS log_comment = '05319_single_token';
-- The second part has no `beta`, so it is pruned and not processed.
SELECT count() FROM tab WHERE hasAllTokens(text, ['alpha', 'beta']) SETTINGS log_comment = '05319_all_tokens';
SELECT count() FROM tab WHERE hasAnyTokens(text, ['beta', 'zeta']) SETTINGS log_comment = '05319_any_tokens';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, read_rows, read_bytes
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05319_single_token', '05319_all_tokens', '05319_any_tokens')
ORDER BY event_time_microseconds;

DROP TABLE tab;

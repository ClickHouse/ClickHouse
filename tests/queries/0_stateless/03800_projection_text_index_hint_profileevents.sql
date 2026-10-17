SET allow_experimental_projection_text_index = 1;
-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_text_index_add_hint = 1;
SET query_plan_direct_read_from_text_index = 1;

-- Tests profile events for text search setting 'query_plan_text_index_add_hint'

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    s String,
    PROJECTION idx_s INDEX s TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab SELECT format('foo {} bar', number) FROM numbers(100000);

SELECT count() FROM tab WHERE s LIKE '%foo%' SETTINGS log_comment = 'hint_pe_test_1';
SELECT count() FROM tab WHERE s LIKE '%7777%' SETTINGS log_comment = 'hint_pe_test_2';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['TextIndexUseHint'] > 0,
    ProfileEvents['TextIndexDiscardHint'] > 0
FROM (
    SELECT * FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('hint_pe_test_1', 'hint_pe_test_2')
    ORDER BY event_time_microseconds DESC
    LIMIT 2
)
ORDER BY event_time_microseconds;

DROP TABLE tab;

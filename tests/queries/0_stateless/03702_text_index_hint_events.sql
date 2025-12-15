-- Tags: no-parallel-replicas
-- Random settings limits: index_granularity=(128, None)

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS t_text_index_hint_events;

CREATE TABLE t_text_index_hint_events
(
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 4
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_text_index_hint_events SELECT format('foo {} bar', number) FROM numbers(100000);

SELECT count() FROM t_text_index_hint_events WHERE s LIKE '%foo%';
SELECT count() FROM t_text_index_hint_events WHERE s LIKE '%7777%';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['TextIndexUseHint'] > 0,
    ProfileEvents['TextIndexDiscardHint'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT count() FROM t_text_index_hint_events%'
ORDER BY event_time_microseconds;

DROP TABLE IF EXISTS t_text_index_hint_events;

-- Tags: no-parallel-replicas
-- Random settings limits: index_granularity=(128, None)

SET enable_analyzer = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_text_index_add_hint = 1;

-- Tests profile events for text search setting 'query_plan_text_index_add_hint'

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    s String,
    INDEX idx_s (s) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO tab SELECT format('foo {} bar', number) FROM numbers(100000);

SELECT count() FROM tab WHERE s LIKE '%foo%';
SELECT count() FROM tab WHERE s LIKE '%7777%';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['TextIndexUseHint'] > 0,
    ProfileEvents['TextIndexDiscardHint'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT count() FROM tab%'
ORDER BY event_time_microseconds;

DROP TABLE tab;

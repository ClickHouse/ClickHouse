-- Tags: no-parallel-replicas
-- no-parallel-replicas: the block-read assertion uses the initiator's ProfileEvents.
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;
SET optimize_rewrite_like_perfect_affix = 0;
SET text_index_like_min_pattern_length = 1;
SET max_threads = 1;
SET log_queries = 1;
SET log_profile_events = 1;

CREATE TABLE text_automaton
(
    id UInt32,
    body String,
    raw String MATERIALIZED body,
    INDEX idx_fc body TYPE text(tokenizer = array, dictionary_block_size = 4, dictionary_block_frontcoding_compression = 1),
    INDEX idx_raw raw TYPE text(tokenizer = array, dictionary_block_size = 4, dictionary_block_frontcoding_compression = 0)
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

-- Many rejected blocks before and after the matching range, in one part.
INSERT INTO text_automaton (id, body)
SELECT number, if(number < 256, concat('aaa', toString(number)), concat('zzz', toString(number))) FROM numbers(512)
UNION ALL SELECT * FROM values('id UInt32, body String',
    (600, 'serviceaerror'), (601, 'serviceberror'), (602, 'servicezok'),
    (603, 'serviceéerror'), (604, 'service\0error'), (605, 'SERVICEaERROR'),
    (606, 'service_literalerror'), (607, 'service%literalerror'),
    (608, 'service\nerror'), (609, 'serviceaerrorTAIL'));
OPTIMIZE TABLE text_automaton FINAL;

-- Internal wildcard and suffix constraints use the automaton, including UTF-8,
-- NUL and newlines. Both dictionary encodings must agree with row evaluation.
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body LIKE 'service_error' SETTINGS log_comment = '05238_automaton';
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE raw LIKE 'service_error';
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body LIKE 'service_error' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body LIKE 'service%error';
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body LIKE 'service%error' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body ILIKE 'SERVICE_ERROR';
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body ILIKE 'SERVICE_ERROR' SETTINGS use_skip_indexes = 0;
SELECT count() FROM text_automaton WHERE body LIKE 'service%missing';
SELECT count() FROM text_automaton WHERE body LIKE 'xxx_error';
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body LIKE 'service\\_%error';
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body LIKE 'service\\_%error' SETTINGS use_skip_indexes = 0;

-- A union of patterns must retain the smaller seek target. Mixing in an infix
-- pattern uses the literal scan; neither case may lose rows.
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body LIKE 'servicea%error' OR body LIKE 'serviceb%error';
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body LIKE 'servicea%error' OR body LIKE 'serviceb%error' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body LIKE 'servicea%error' OR body LIKE '%literal%';
SELECT arraySort(groupArray(id)) FROM text_automaton WHERE body LIKE 'servicea%error' OR body LIKE '%literal%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;
SELECT count() > 0 AND max(ProfileEvents['TextIndexReadDictionaryBlocks']) BETWEEN 1 AND 8
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05238_automaton'
    AND event_date >= yesterday() AND event_time >= now() - 600;

-- Exceed the existing non-embedded-postings budget. Partial enumeration must
-- trigger the established query bypass, never produce partial results.
TRUNCATE TABLE text_automaton;
INSERT INTO text_automaton (id, body) SELECT number, concat('service', toString(number % 4), 'error') FROM numbers(80);
SELECT count() FROM text_automaton WHERE body LIKE 'service_error' SETTINGS text_index_like_max_postings_to_read = 1;
SELECT count() FROM text_automaton WHERE body LIKE 'service_error' SETTINGS use_skip_indexes = 0;
DROP TABLE text_automaton;

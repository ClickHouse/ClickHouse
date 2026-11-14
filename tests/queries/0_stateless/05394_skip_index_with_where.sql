-- Tags: no-random-settings
-- Partial skip indexes: `INDEX ... WHERE cond` indexes only the rows satisfying `cond`
-- and is used only when the query filter has `cond` as a conjunct.

SET allow_experimental_partial_skip_indexes = 1;
SET use_skip_indexes = 1;
-- `max_rows_to_read` must see the ranges left after skip index analysis.
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET parallel_replicas_index_analysis_only_on_coordinator = 0;

DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS logs_misc;
DROP TABLE IF EXISTS versions;

CREATE TABLE logs
(
    id UInt64,
    level String,
    message String,
    INDEX message_idx message TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1 WHERE level = 'Error',
    INDEX id_minmax id TYPE minmax WHERE level = 'Error',
    INDEX id_set id TYPE set(10) WHERE level = 'Error',
    INDEX id_bloom id TYPE bloom_filter WHERE level = 'Error'
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 10, index_granularity_bytes = '10Mi';

-- Every 100th row is an `Error` row; all `Info` rows and the `Error` row 500 say 'boom'.
INSERT INTO logs SELECT number, if(number % 100 = 0, 'Error', 'Info'), if(number % 100 = 0 AND number != 500, 'calm', 'boom') FROM numbers(1000);

SHOW CREATE TABLE logs;

-- { echoOn }

-- The filter implies the index condition: granules without matching `Error` rows are skipped.
SELECT id FROM logs WHERE level = 'Error' AND hasToken(message, 'boom') SETTINGS force_data_skipping_indices = 'message_idx', max_rows_to_read = 10;
SELECT id FROM logs WHERE id >= 900 AND level = 'Error' SETTINGS force_data_skipping_indices = 'id_minmax', max_rows_to_read = 10;
SELECT count() FROM logs WHERE level = 'Error' AND id = 301 SETTINGS ignore_data_skipping_indices = 'id_minmax,id_bloom', force_data_skipping_indices = 'id_set', max_rows_to_read = 1;
SELECT count() FROM logs WHERE level = 'Error' AND id = 301 SETTINGS ignore_data_skipping_indices = 'id_minmax,id_set', force_data_skipping_indices = 'id_bloom';

-- The filter does not imply the index condition: the index is not used.
SELECT count() FROM logs WHERE hasToken(message, 'boom');
SELECT count() FROM logs WHERE hasToken(message, 'boom') SETTINGS force_data_skipping_indices = 'message_idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM logs WHERE (level = 'Error' OR id = 1) AND hasToken(message, 'boom') SETTINGS force_data_skipping_indices = 'message_idx'; -- { serverError INDEX_NOT_USED }

-- A partial `minmax` index is not used for top-K reading.
SELECT id FROM logs WHERE level = 'Error' ORDER BY id DESC LIMIT 3 SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1;

-- Updating a column of the condition rebuilds the index.
ALTER TABLE logs UPDATE level = 'Error' WHERE id = 301 SETTINGS mutations_sync = 2;
SELECT id FROM logs WHERE level = 'Error' AND id = 301 SETTINGS force_data_skipping_indices = 'id_set';

-- Merges rebuild the index from the merged rows.
INSERT INTO logs VALUES (1000, 'Error', 'boom'), (1001, 'Info', 'boom');
OPTIMIZE TABLE logs FINAL;
SELECT id FROM logs WHERE level = 'Error' AND hasToken(message, 'boom') ORDER BY id SETTINGS force_data_skipping_indices = 'message_idx';

-- `ADD INDEX` and `CREATE INDEX` accept `WHERE`, and the definition survives a reattach.
ALTER TABLE logs ADD INDEX id_minmax_info id TYPE minmax GRANULARITY 1 WHERE level = 'Info';
CREATE INDEX message_ngram ON logs (message) TYPE ngrambf_v1(3, 8192, 3, 0) GRANULARITY 1 WHERE level = 'Error' AND id > 100;
ALTER TABLE logs MATERIALIZE INDEX id_minmax_info SETTINGS mutations_sync = 2;
ALTER TABLE logs MATERIALIZE INDEX message_ngram SETTINGS mutations_sync = 2;
DETACH TABLE logs;
ATTACH TABLE logs;
SHOW CREATE TABLE logs;
SELECT count() FROM logs WHERE level = 'Info' AND id > 995 SETTINGS force_data_skipping_indices = 'id_minmax_info', max_rows_to_read = 12;
SELECT id FROM logs WHERE id > 100 AND level = 'Error' AND message LIKE '%boo%' ORDER BY id SETTINGS force_data_skipping_indices = 'message_ngram';
SELECT id FROM logs WHERE level = 'Error' AND message LIKE '%boo%' ORDER BY id SETTINGS force_data_skipping_indices = 'message_ngram'; -- { serverError INDEX_NOT_USED }

-- An `IN` condition.
CREATE TABLE logs_misc
(
    id UInt64,
    level String,
    INDEX id_in id TYPE minmax WHERE level IN ('Error', 'Fatal')
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 10, index_granularity_bytes = '10Mi';
INSERT INTO logs_misc SELECT number, if(number % 100 = 0, 'Error', 'Info') FROM numbers(1000);
SELECT count() FROM logs_misc WHERE level IN ('Error', 'Fatal') AND id = 300 SETTINGS force_data_skipping_indices = 'id_in', max_rows_to_read = 10;

-- With FINAL, the granule with the newest version is read even if it has no rows for the index.
CREATE TABLE versions (id UInt64, status String, INDEX id_active id TYPE minmax WHERE status = 'active')
ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO versions VALUES (1, 'active');
INSERT INTO versions VALUES (1, 'deleted');
SELECT count() FROM versions FINAL WHERE status = 'active' AND id = 1 SETTINGS use_skip_indexes_if_final = 1, use_skip_indexes_if_final_exact_mode = 1;

-- { echoOff }

CREATE TABLE logs_array_join (x UInt64, INDEX i x TYPE minmax WHERE arrayJoin([1, 1]) = 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE logs_constant (x UInt64, INDEX i x TYPE minmax WHERE 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError INCORRECT_QUERY }
CREATE TABLE logs_text (s String, INDEX i s TYPE text(tokenizer = 'splitByNonAlpha') WHERE s = 'a') ENGINE = MergeTree ORDER BY tuple(); -- { serverError NOT_IMPLEMENTED }
CREATE TABLE logs_not_bool (s String, INDEX i s TYPE minmax WHERE s) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
CREATE TABLE logs_alias (s String, INDEX i s TYPE minmax WHERE (s = 'a' AS is_a)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

DROP TABLE logs;
DROP TABLE logs_misc;
DROP TABLE versions;

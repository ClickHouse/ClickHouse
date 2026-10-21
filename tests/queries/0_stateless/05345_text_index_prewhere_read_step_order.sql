-- Virtual columns of a text index are read right before the first PREWHERE step that uses them,
-- so the index is read only for the granules that passed the preceding steps (including the TopK filter).
-- Without that, the prewhere readers would read every selected row twice: once for `flag`/`ts`, once from the index.

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
-- Analyze the indexes in advance, so that only the PREWHERE readers count in `RowsReadByPrewhereReaders`.
SET use_skip_indexes_on_data_read = 0;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_count_from_text_index = 0;
SET use_query_condition_cache = 0;
SET enable_multiple_prewhere_read_steps = 1;
SET max_threads = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_partial;

CREATE TABLE tab
(
    id UInt64,
    flag UInt8,
    ts UInt64,
    s String,
    t String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_t t TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';

-- `flag` passes whole granules (every 10th one), `third` and `common` are in every granule,
-- `ts` decreases, so the TopK threshold set by the first block rejects all later rows.
INSERT INTO tab SELECT
    number,
    intDiv(number, 128) % 10 = 0,
    1000000 - number,
    concat('common', if(number % 3 = 0, ' third', '')),
    if(number % 5 = 0, 'fifth', 'other')
FROM numbers(100000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

SELECT count(), sum(id) FROM tab PREWHERE flag = 1 AND hasToken(s, 'third') SETTINGS log_comment = 'read_order_flag_first';
SELECT count(), sum(id) FROM tab PREWHERE hasToken(s, 'third') AND flag = 1 SETTINGS log_comment = 'read_order_index_first';
SELECT count(), sum(id) FROM tab PREWHERE flag = 1 WHERE hasToken(s, 'third') SETTINGS optimize_move_to_prewhere = 0, log_comment = 'read_order_index_in_where';
SELECT id FROM tab WHERE hasToken(s, 'third') ORDER BY ts DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 1, max_block_size = 1024, log_comment = 'read_order_top_k';

SYSTEM FLUSH LOGS query_log;

-- 1 if the index was read only for the rows that passed the preceding steps.
SELECT log_comment, ProfileEvents['RowsReadByPrewhereReaders'] < 1.5 * ProfileEvents['SelectedRows']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'read\_order\_%'
ORDER BY log_comment;

SELECT 'Same results as without direct read';

SELECT count(), sum(id) FROM tab PREWHERE hasToken(s, 'common') AND flag = 1 AND hasToken(s, 'third');
SELECT count(), sum(id) FROM tab PREWHERE hasToken(s, 'common') AND flag = 1 AND hasToken(s, 'third') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT count(), sum(id) FROM tab WHERE flag = 1 AND (hasToken(s, 'third') OR hasToken(t, 'fifth'));
SELECT count(), sum(id) FROM tab WHERE flag = 1 AND (hasToken(s, 'third') OR hasToken(t, 'fifth')) SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT id FROM tab WHERE hasToken(s, 'third') ORDER BY ts DESC LIMIT 3 SETTINGS query_plan_direct_read_from_text_index = 0;

-- The index is materialized only in the second part: the first part computes the virtual column from `s`.
CREATE TABLE tab_partial
(
    id UInt64,
    flag UInt8,
    s String
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';

INSERT INTO tab_partial SELECT number, intDiv(number, 128) % 10 = 0, concat('common', if(number % 3 = 0, ' third', '')) FROM numbers(10000);
ALTER TABLE tab_partial ADD INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha);
INSERT INTO tab_partial SELECT number, intDiv(number, 128) % 10 = 0, concat('common', if(number % 3 = 0, ' third', '')) FROM numbers(10000, 10000);

SELECT count(), sum(id) FROM tab_partial PREWHERE length(s) > 6 AND flag = 1 AND hasToken(s, 'third');
SELECT count(), sum(id) FROM tab_partial PREWHERE length(s) > 6 AND flag = 1 AND hasToken(s, 'third') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;
DROP TABLE tab_partial;

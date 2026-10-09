-- Planning must not resolve the pending patches of every part when the query analyzed no text index.
-- `collectTextIndexReadInfos` builds alter conversions for each part -- work proportional to
-- (parts x patch parts) -- and that result only ever feeds text index entries. It used to run before
-- checking whether any text index was analyzed at all, so a query using only a `bloom_filter` index on a
-- table with pending patch parts paid the whole walk for nothing.

SET max_rows_to_read = 0; -- system.text_log can be really big
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_patch_no_text_index;
DROP TABLE IF EXISTS t_patch_with_text_index;

CREATE TABLE t_patch_no_text_index (k UInt64, s String, v UInt64, INDEX idx_bf s TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_patch_no_text_index;
INSERT INTO t_patch_no_text_index VALUES (1, 'alpha', 0), (2, 'beta', 0);
UPDATE t_patch_no_text_index SET v = 1 WHERE k = 1;

SELECT count() FROM t_patch_no_text_index WHERE s = 'alpha' SETTINGS log_comment = '05345_no_text_index';

-- Positive control: with a text index the check must still run and report the pending patch, otherwise
-- the assertion above would pass even if the message were simply never emitted.
CREATE TABLE t_patch_with_text_index (k UInt64, s String, v UInt64, INDEX idx_text s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_patch_with_text_index;
INSERT INTO t_patch_with_text_index VALUES (1, 'alpha', 0), (2, 'beta', 0);
UPDATE t_patch_with_text_index SET v = 1 WHERE k = 1;

SELECT count() FROM t_patch_with_text_index WHERE hasToken(s, 'alpha')
SETTINGS log_comment = '05345_with_text_index', query_plan_direct_read_from_text_index = 1, use_skip_indexes_on_data_read = 1;

SYSTEM FLUSH LOGS query_log, text_log;

SELECT 'patch walk without text index', count()
FROM system.text_log
WHERE query_id IN (
        SELECT query_id FROM system.query_log
        WHERE log_comment = '05345_no_text_index' AND current_database = currentDatabase() AND type = 'QueryFinish')
  AND message_format_string = 'Cannot use direct reading from text index. Reason: a part has a pending patch';

SELECT 'patch walk with text index', count() > 0
FROM system.text_log
WHERE query_id IN (
        SELECT query_id FROM system.query_log
        WHERE log_comment = '05345_with_text_index' AND current_database = currentDatabase() AND type = 'QueryFinish')
  AND message_format_string = 'Cannot use direct reading from text index. Reason: a part has a pending patch';

DROP TABLE t_patch_no_text_index;
DROP TABLE t_patch_with_text_index;

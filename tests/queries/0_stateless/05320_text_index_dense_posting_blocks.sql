-- Blocks of a compressed posting list that hold consecutive row ids are added to the posting list as one range each.
-- Every result is compared with a plain scan of the column, for `bitpacking` and `pfor` indexes (and a
-- `none` control), for tokens present in every row, in every row but one (inside a block, at a block boundary, at a
-- segment boundary), in a run inside the part, in every second row, in a single segment, and in a segment of one row.
-- The profile event `TextIndexDensePackedBlocks` counts the blocks added as ranges.

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET use_text_index_postings_cache = 0;
SET enable_parallel_replicas = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS tab_none;
DROP TABLE IF EXISTS tab_bitpacking;
DROP TABLE IF EXISTS tab_pfor;

-- A segment holds 1024 row ids, that is 8 blocks of 128.
CREATE TABLE tab_none
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'none', posting_list_block_size = 1024) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE tab_bitpacking
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 1024) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE tab_pfor
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'pfor', posting_list_block_size = 1024) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

--   aall   : every row
--   bgap   : every row but 5000 (inside a block)
--   cedge  : every row but 128 (the next block starts after a gap)
--   dseg   : every row but 1024 (the next segment starts after a gap)
--   erun   : rows [3000, 7000)
--   fhalf  : every second row
--   gsmall : rows [2000, 2900), a single segment
--   hone   : rows [0, 1025), the last segment holds one row
INSERT INTO tab_none
SELECT number,
    concat('aall',
        if(number != 5000, ' bgap', ''),
        if(number != 128, ' cedge', ''),
        if(number != 1024, ' dseg', ''),
        if(number >= 3000 AND number < 7000, ' erun', ''),
        if(number % 2 = 0, ' fhalf', ''),
        if(number >= 2000 AND number < 2900, ' gsmall', ''),
        if(number < 1025, ' hone', ''))
FROM numbers(10000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

INSERT INTO tab_bitpacking SELECT * FROM tab_none
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

INSERT INTO tab_pfor SELECT * FROM tab_none
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

SELECT 'parts', table, count() FROM system.parts WHERE database = currentDatabase() AND table LIKE 'tab_%' AND active GROUP BY table ORDER BY table;

SELECT 'Ground truth';
SET use_skip_indexes = 0;
SELECT 'aall', count(), sum(id) FROM tab_none WHERE hasToken(s, 'aall');
SELECT 'bgap', count(), sum(id) FROM tab_none WHERE hasToken(s, 'bgap');
SELECT 'cedge', count(), sum(id) FROM tab_none WHERE hasToken(s, 'cedge');
SELECT 'dseg', count(), sum(id) FROM tab_none WHERE hasToken(s, 'dseg');
SELECT 'erun', count(), sum(id) FROM tab_none WHERE hasToken(s, 'erun');
SELECT 'fhalf', count(), sum(id) FROM tab_none WHERE hasToken(s, 'fhalf');
SELECT 'gsmall', count(), sum(id) FROM tab_none WHERE hasToken(s, 'gsmall');
SELECT 'hone', count(), sum(id) FROM tab_none WHERE hasToken(s, 'hone');

SELECT 'Materialize';
SET use_skip_indexes = 1;
SET text_index_posting_list_apply_mode = 'materialize';
SET query_plan_optimize_count_from_text_index = 0;
SELECT 'none', 'aall', count(), sum(id) FROM tab_none WHERE hasToken(s, 'aall') SETTINGS log_comment = '05320_none_aall';
SELECT 'bitpacking', 'aall', count(), sum(id) FROM tab_bitpacking WHERE hasToken(s, 'aall') SETTINGS log_comment = '05320_bitpacking_aall';
SELECT 'bitpacking', 'bgap', count(), sum(id) FROM tab_bitpacking WHERE hasToken(s, 'bgap') SETTINGS log_comment = '05320_bitpacking_bgap';
SELECT 'bitpacking', 'cedge', count(), sum(id) FROM tab_bitpacking WHERE hasToken(s, 'cedge') SETTINGS log_comment = '05320_bitpacking_cedge';
SELECT 'bitpacking', 'dseg', count(), sum(id) FROM tab_bitpacking WHERE hasToken(s, 'dseg') SETTINGS log_comment = '05320_bitpacking_dseg';
SELECT 'bitpacking', 'erun', count(), sum(id) FROM tab_bitpacking WHERE hasToken(s, 'erun') SETTINGS log_comment = '05320_bitpacking_erun';
SELECT 'bitpacking', 'fhalf', count(), sum(id) FROM tab_bitpacking WHERE hasToken(s, 'fhalf') SETTINGS log_comment = '05320_bitpacking_fhalf';
SELECT 'bitpacking', 'gsmall', count(), sum(id) FROM tab_bitpacking WHERE hasToken(s, 'gsmall') SETTINGS log_comment = '05320_bitpacking_gsmall';
SELECT 'bitpacking', 'hone', count(), sum(id) FROM tab_bitpacking WHERE hasToken(s, 'hone') SETTINGS log_comment = '05320_bitpacking_hone';
SELECT 'bitpacking', 'all', count(), sum(id) FROM tab_bitpacking WHERE hasAllTokens(s, ['cedge', 'fhalf']);
SELECT 'bitpacking', 'any', count(), sum(id) FROM tab_bitpacking WHERE hasAnyTokens(s, ['erun', 'gsmall']);
SELECT 'pfor', 'aall', count(), sum(id) FROM tab_pfor WHERE hasToken(s, 'aall') SETTINGS log_comment = '05320_pfor_aall';
SELECT 'pfor', 'bgap', count(), sum(id) FROM tab_pfor WHERE hasToken(s, 'bgap') SETTINGS log_comment = '05320_pfor_bgap';
SELECT 'pfor', 'cedge', count(), sum(id) FROM tab_pfor WHERE hasToken(s, 'cedge') SETTINGS log_comment = '05320_pfor_cedge';
SELECT 'pfor', 'dseg', count(), sum(id) FROM tab_pfor WHERE hasToken(s, 'dseg') SETTINGS log_comment = '05320_pfor_dseg';
SELECT 'pfor', 'erun', count(), sum(id) FROM tab_pfor WHERE hasToken(s, 'erun') SETTINGS log_comment = '05320_pfor_erun';
SELECT 'pfor', 'fhalf', count(), sum(id) FROM tab_pfor WHERE hasToken(s, 'fhalf') SETTINGS log_comment = '05320_pfor_fhalf';
SELECT 'pfor', 'gsmall', count(), sum(id) FROM tab_pfor WHERE hasToken(s, 'gsmall') SETTINGS log_comment = '05320_pfor_gsmall';
SELECT 'pfor', 'hone', count(), sum(id) FROM tab_pfor WHERE hasToken(s, 'hone') SETTINGS log_comment = '05320_pfor_hone';
SELECT 'pfor', 'all', count(), sum(id) FROM tab_pfor WHERE hasAllTokens(s, ['cedge', 'fhalf']);
SELECT 'pfor', 'any', count(), sum(id) FROM tab_pfor WHERE hasAnyTokens(s, ['erun', 'gsmall']);

SELECT 'Lazy';
SELECT 'lazy', 'pfor', 'gsmall', count(), sum(id) FROM tab_pfor WHERE hasToken(s, 'gsmall')
SETTINGS text_index_posting_list_apply_mode = 'lazy', log_comment = '05320_lazy_pfor_gsmall';

SELECT 'Count';
SET text_index_posting_list_apply_mode = 'lazy';
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SELECT 'bitpacking', count() FROM tab_bitpacking WHERE hasAllTokens(s, ['aall', 'bgap']);
SELECT 'bitpacking', count() FROM tab_bitpacking WHERE hasAllTokens(s, ['cedge', 'fhalf']);
SELECT 'bitpacking', count() FROM tab_bitpacking WHERE hasAnyTokens(s, ['erun', 'gsmall']);
SELECT 'pfor', count() FROM tab_pfor WHERE hasAllTokens(s, ['aall', 'bgap']);
SELECT 'pfor', count() FROM tab_pfor WHERE hasAllTokens(s, ['cedge', 'fhalf']);
SELECT 'pfor', count() FROM tab_pfor WHERE hasAnyTokens(s, ['erun', 'gsmall']);

SYSTEM FLUSH LOGS query_log;

-- Number of blocks added as ranges per query.
WITH initial_queries AS
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND is_initial_query = 1
      AND match(log_comment, '^05320_(bitpacking|pfor|none|lazy)_')
)
SELECT q.log_comment, sum(ProfileEvents['TextIndexDensePackedBlocks'])
FROM system.query_log AS l
INNER JOIN initial_queries AS q ON l.initial_query_id = q.query_id
WHERE l.event_date >= yesterday() AND l.event_time >= now() - 600
  AND l.type = 'QueryFinish'
GROUP BY q.log_comment
ORDER BY q.log_comment;

DROP TABLE tab_none;
DROP TABLE tab_bitpacking;
DROP TABLE tab_pfor;

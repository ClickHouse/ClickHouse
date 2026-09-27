-- A posting list of a token present in every row of its row range is built from that range instead of being read from
-- the text index. Every result is compared with a plain scan of the column, for `none` and `pfor` indexes, in the
-- `count()` read and in the materializing read, for tokens present in every row, in every row but one, in one run inside
-- the part, in two runs that meet at a posting list block boundary, and in short runs. The profile events
-- `TextIndexDensePostingsBuiltFromRanges` and `TextIndexReadPostings` show which posting lists were built from their row
-- ranges and which were read.

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET use_text_index_postings_cache = 0;

DROP TABLE IF EXISTS tab_dense_none;
DROP TABLE IF EXISTS tab_dense_pfor;

-- Posting lists are split into blocks of 1024 row ids, so the frequent tokens have many blocks.
CREATE TABLE tab_dense_none
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'none', posting_list_block_size = 1024) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

CREATE TABLE tab_dense_pfor
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'pfor', posting_list_block_size = 1024) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

--   aall   : every row
--   bgap   : every row but 50001
--   chead  : every row but 0
--   eblk   : rows [20000, 30000)
--   fseg   : rows [0, 1024) and [1025, 2049), one block each
--   graw   : rows [500, 510), short enough to be stored as raw row ids
--   jone   : rows 7, 77 and 777, embedded into the dictionary
--   kcross : rows [65000, 66000), one block
INSERT INTO tab_dense_none
SELECT number,
    concat('aall',
        if(number != 50001, ' bgap', ''),
        if(number != 0, ' chead', ''),
        if(number >= 20000 AND number < 30000, ' eblk', ''),
        if(number < 1024 OR (number > 1024 AND number < 2049), ' fseg', ''),
        if(number >= 500 AND number < 510, ' graw', ''),
        if(number IN (7, 77, 777), ' jone', ''),
        if(number >= 65000 AND number < 66000, ' kcross', ''))
FROM numbers(100000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

INSERT INTO tab_dense_pfor SELECT * FROM tab_dense_none
SETTINGS max_threads = 1, max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

SELECT 'parts', table, count() FROM system.parts WHERE database = currentDatabase() AND table LIKE 'tab_dense_%' AND active GROUP BY table ORDER BY table;

SELECT 'Ground truth';
SET use_skip_indexes = 0;
SELECT 'aall', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'aall');
SELECT 'bgap', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'bgap');
SELECT 'chead', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'chead');
SELECT 'eblk', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'eblk');
SELECT 'fseg', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'fseg');
SELECT 'graw', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'graw');
SELECT 'kcross', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'kcross');
SELECT 'any', count(), sum(id) FROM tab_dense_none WHERE hasAnyTokens(s, ['eblk', 'kcross', 'jone']);

SELECT 'Count';
SET use_skip_indexes = 1;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET serialize_query_plan = 0;
-- Parallel replicas turn the count step off, so the plan check and the logged count below run without them.
SELECT count() > 0 FROM (EXPLAIN SELECT 'pfor', 'chead', count() FROM tab_dense_pfor WHERE hasAllTokens(s, ['chead', 'aall']) SETTINGS enable_parallel_replicas = 0) WHERE explain ILIKE '%ReadFromTextIndexCount%';
-- `aall` is in every row, so each count is the count of the other token.
SELECT 'none', 'bgap', count() FROM tab_dense_none WHERE hasAllTokens(s, ['bgap', 'aall']);
SELECT 'none', 'chead', count() FROM tab_dense_none WHERE hasAllTokens(s, ['chead', 'aall']);
SELECT 'none', 'eblk', count() FROM tab_dense_none WHERE hasAllTokens(s, ['eblk', 'aall']);
SELECT 'none', 'fseg', count() FROM tab_dense_none WHERE hasAllTokens(s, ['fseg', 'aall']);
SELECT 'none', 'graw', count() FROM tab_dense_none WHERE hasAllTokens(s, ['graw', 'aall']);
SELECT 'none', 'kcross', count() FROM tab_dense_none WHERE hasAllTokens(s, ['kcross', 'aall']);
SELECT 'none', 'any', count() FROM tab_dense_none WHERE hasAnyTokens(s, ['eblk', 'kcross', 'jone']);
SELECT 'pfor', 'bgap', count() FROM tab_dense_pfor WHERE hasAllTokens(s, ['bgap', 'aall']);
SELECT 'pfor', 'chead', count() FROM tab_dense_pfor WHERE hasAllTokens(s, ['chead', 'aall']) SETTINGS enable_parallel_replicas = 0, log_comment = '05260_count_pfor_chead';
SELECT 'pfor', 'eblk', count() FROM tab_dense_pfor WHERE hasAllTokens(s, ['eblk', 'aall']);
SELECT 'pfor', 'fseg', count() FROM tab_dense_pfor WHERE hasAllTokens(s, ['fseg', 'aall']);
SELECT 'pfor', 'graw', count() FROM tab_dense_pfor WHERE hasAllTokens(s, ['graw', 'aall']);
SELECT 'pfor', 'kcross', count() FROM tab_dense_pfor WHERE hasAllTokens(s, ['kcross', 'aall']);
SELECT 'pfor', 'any', count() FROM tab_dense_pfor WHERE hasAnyTokens(s, ['eblk', 'kcross', 'jone']);

SELECT 'Materialize';
SET query_plan_optimize_count_from_text_index = 0;
SET text_index_posting_list_apply_mode = 'materialize';
SELECT 'none', 'aall', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'aall') SETTINGS log_comment = '05260_none_aall';
SELECT 'none', 'bgap', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'bgap') SETTINGS log_comment = '05260_none_bgap';
SELECT 'none', 'chead', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'chead') SETTINGS log_comment = '05260_none_chead';
SELECT 'none', 'eblk', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'eblk') SETTINGS log_comment = '05260_none_eblk';
SELECT 'none', 'fseg', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'fseg') SETTINGS log_comment = '05260_none_fseg';
SELECT 'none', 'graw', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'graw') SETTINGS log_comment = '05260_none_graw';
SELECT 'none', 'kcross', count(), sum(id) FROM tab_dense_none WHERE hasToken(s, 'kcross') SETTINGS log_comment = '05260_none_kcross';
SELECT 'none', 'any', count(), sum(id) FROM tab_dense_none WHERE hasAnyTokens(s, ['eblk', 'kcross', 'jone']);
SELECT 'pfor', 'aall', count(), sum(id) FROM tab_dense_pfor WHERE hasToken(s, 'aall') SETTINGS log_comment = '05260_pfor_aall';
SELECT 'pfor', 'bgap', count(), sum(id) FROM tab_dense_pfor WHERE hasToken(s, 'bgap') SETTINGS log_comment = '05260_pfor_bgap';
SELECT 'pfor', 'chead', count(), sum(id) FROM tab_dense_pfor WHERE hasToken(s, 'chead') SETTINGS log_comment = '05260_pfor_chead';
SELECT 'pfor', 'eblk', count(), sum(id) FROM tab_dense_pfor WHERE hasToken(s, 'eblk') SETTINGS log_comment = '05260_pfor_eblk';
SELECT 'pfor', 'fseg', count(), sum(id) FROM tab_dense_pfor WHERE hasToken(s, 'fseg') SETTINGS log_comment = '05260_pfor_fseg';
SELECT 'pfor', 'graw', count(), sum(id) FROM tab_dense_pfor WHERE hasToken(s, 'graw') SETTINGS log_comment = '05260_pfor_graw';
SELECT 'pfor', 'kcross', count(), sum(id) FROM tab_dense_pfor WHERE hasToken(s, 'kcross') SETTINGS log_comment = '05260_pfor_kcross';
SELECT 'pfor', 'any', count(), sum(id) FROM tab_dense_pfor WHERE hasAnyTokens(s, ['eblk', 'kcross', 'jone']);

SYSTEM FLUSH LOGS query_log;

-- The posting lists of the tokens present in every row of their row range are built from it, and only the others are read.
WITH initial_queries AS
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND is_initial_query = 1
      AND match(log_comment, '^05260_(count_pfor|none|pfor)_')
)
SELECT
    q.log_comment,
    sum(ProfileEvents['TextIndexDensePostingsBuiltFromRanges']) > 0 AS built_from_ranges,
    sum(ProfileEvents['TextIndexReadPostings']) > 0 AS read
FROM system.query_log AS l
INNER JOIN initial_queries AS q ON l.initial_query_id = q.query_id
WHERE l.event_date >= yesterday() AND l.event_time >= now() - 600
  AND l.type = 'QueryFinish'
GROUP BY q.log_comment
ORDER BY q.log_comment;

DROP TABLE tab_dense_none;
DROP TABLE tab_dense_pfor;

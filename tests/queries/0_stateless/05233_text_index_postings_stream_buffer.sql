-- Posting lists are read through streams whose buffer is sized to the segments they read: the cursors of the
-- lazy posting list apply mode, the analysis that folds single-segment lists, the count computed from the index
-- and the merge of parts with a text index. This test pins the results of those paths on multi-segment and
-- single-segment lists, in two parts and after their merge. The effect itself - the number of reads a large
-- posting list takes - is pinned by `05234_text_index_postings_stream_buffer_reads`.

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET use_text_index_postings_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS t_postings_buffer;

-- posting_list_block_size = 1024: per part, `every` (20000 postings) and `half` (10000) span many segments,
-- `tenth` (2000) two, `hundredth` (200) is a single segment read by the analysis, `rare` (2) is embedded.
CREATE TABLE t_postings_buffer
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', posting_list_block_size = 1024)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES t_postings_buffer;

INSERT INTO t_postings_buffer
SELECT number, concat('every', if(number % 2 = 0, ' half', ''), if(number % 10 = 0, ' tenth', ''), if(number % 100 = 0, ' hundredth', ''), if(number % 10000 = 1, ' rare', ''))
FROM numbers(20000);

INSERT INTO t_postings_buffer
SELECT number, concat('every', if(number % 2 = 0, ' half', ''), if(number % 10 = 0, ' tenth', ''), if(number % 100 = 0, ' hundredth', ''), if(number % 10000 = 1, ' rare', ''))
FROM numbers(20000, 20000);

SELECT 'lazy, direct read, two parts';
SET text_index_posting_list_apply_mode = 'lazy';
SET query_plan_optimize_count_from_text_index = 0;
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasToken(s, 'half');
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'tenth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'hundredth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAnyTokens(s, ['tenth', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['every', 'rare']);

SELECT 'lazy, analysis only, two parts';
SET use_skip_indexes_on_data_read = 0;
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasToken(s, 'half');
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'tenth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'hundredth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAnyTokens(s, ['tenth', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['every', 'rare']);
SET use_skip_indexes_on_data_read = 1;

SELECT 'materialize, two parts';
SET text_index_posting_list_apply_mode = 'materialize';
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasToken(s, 'half');
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'tenth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'hundredth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAnyTokens(s, ['tenth', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['every', 'rare']);
SET text_index_posting_list_apply_mode = 'lazy';

SELECT 'count from the index, two parts';
SET query_plan_optimize_count_from_text_index = 1;
SELECT count() FROM t_postings_buffer WHERE hasToken(s, 'half');
SELECT count() FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'tenth']);
SELECT count() FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'hundredth']);
SELECT count() FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'rare']);
SELECT count() FROM t_postings_buffer WHERE hasAnyTokens(s, ['tenth', 'rare']);
SELECT count() FROM t_postings_buffer WHERE hasAllTokens(s, ['every', 'rare']);
SET query_plan_optimize_count_from_text_index = 0;

-- The merge reads the posting lists of both parts through the postings streams of the merge task.
SELECT 'merged into one part';
SYSTEM START MERGES t_postings_buffer;
OPTIMIZE TABLE t_postings_buffer FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_postings_buffer' AND active;

SELECT 'lazy, direct read, one part';
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasToken(s, 'half');
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'tenth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'hundredth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAnyTokens(s, ['tenth', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['every', 'rare']);

SELECT 'lazy, analysis only, one part';
SET use_skip_indexes_on_data_read = 0;
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasToken(s, 'half');
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'tenth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'hundredth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAnyTokens(s, ['tenth', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['every', 'rare']);
SET use_skip_indexes_on_data_read = 1;

SELECT 'materialize, one part';
SET text_index_posting_list_apply_mode = 'materialize';
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasToken(s, 'half');
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'tenth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'hundredth']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAnyTokens(s, ['tenth', 'rare']);
SELECT count(), sum(id) FROM t_postings_buffer WHERE hasAllTokens(s, ['every', 'rare']);
SET text_index_posting_list_apply_mode = 'lazy';

SELECT 'count from the index, one part';
SET query_plan_optimize_count_from_text_index = 1;
SELECT count() FROM t_postings_buffer WHERE hasToken(s, 'half');
SELECT count() FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'tenth']);
SELECT count() FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'hundredth']);
SELECT count() FROM t_postings_buffer WHERE hasAllTokens(s, ['half', 'rare']);
SELECT count() FROM t_postings_buffer WHERE hasAnyTokens(s, ['tenth', 'rare']);
SELECT count() FROM t_postings_buffer WHERE hasAllTokens(s, ['every', 'rare']);
SET query_plan_optimize_count_from_text_index = 0;

DROP TABLE t_postings_buffer;

-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-distributed-cache
-- no-fasttest: needs an S3 disk.
-- no-random-settings, no-random-merge-tree-settings: pins page_cache_block_size and the text index's own block sizes.
-- no-distributed-cache: with the distributed cache, the page cache is used only with use_page_cache_with_distributed_cache, and reads do not use the disk connections checked here.

-- Text index readers visit blocks out of file order, so `use_page_cache_lookahead_for_text_index`
-- keeps their lookahead at one block by default. Query A is the default; query B flips the setting
-- on; query C is a plain sequential scan, unaffected either way.

DROP TABLE IF EXISTS t_text_index_page_cache_lookahead;
DROP TABLE IF EXISTS t_text_index_page_cache_lookahead_seq;

-- 40 tokens, 8 of them (searched below) with much larger cardinalities than the 32 filler tokens.
-- A hash shuffle interleaves every token's rows, so its own postings cannot compress into one short run.
CREATE TABLE t_text_index_page_cache_lookahead
(
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 4, posting_list_block_size = 256)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0,
    min_compress_block_size = 256, max_compress_block_size = 256;

INSERT INTO t_text_index_page_cache_lookahead (s)
SELECT concat('kwneedle', leftPad(toString(token_idx_and_rows.1), 2, '0'))
FROM
(
    SELECT arrayJoin(arrayZip(
        range(40),
        [2500, 1000, 1000, 1000, 1000, 15000, 1000, 1000, 1000, 1000,
         7500, 1000, 1000, 1000, 1000, 20000, 1000, 1000, 1000, 1000,
         12500, 1000, 1000, 1000, 1000, 5000, 1000, 1000, 1000, 1000,
         17500, 1000, 1000, 1000, 1000, 10000, 1000, 1000, 1000, 1000]
    )) AS token_idx_and_rows
) AS tokens
ARRAY JOIN range(token_idx_and_rows.2) AS row_in_token
ORDER BY sipHash64(row_in_token, token_idx_and_rows.1);

OPTIMIZE TABLE t_text_index_page_cache_lookahead FINAL;

-- A plain column, read sequentially: the control, unaffected by the text index change.
CREATE TABLE t_text_index_page_cache_lookahead_seq (n UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0,
    min_compress_block_size = 256, max_compress_block_size = 256;

INSERT INTO t_text_index_page_cache_lookahead_seq SELECT sipHash64(number) FROM numbers(150000);

OPTIMIZE TABLE t_text_index_page_cache_lookahead_seq FINAL;

SYSTEM DROP PAGE CACHE;

-- The 8 tokens are listed in file order, but are actually read rarest-first; the *_cache settings
-- disable the server-wide text index caches, so the query reads through the page cache.
SELECT count() FROM t_text_index_page_cache_lookahead
WHERE hasAnyTokens(s, ['kwneedle00', 'kwneedle05', 'kwneedle10', 'kwneedle15', 'kwneedle20', 'kwneedle25', 'kwneedle30', 'kwneedle35'])
SETTINGS
    enable_full_text_index = 1,
    use_page_cache_for_disks_without_file_cache = 1,
    page_cache_block_size = 1024,
    max_threads = 1,
    use_text_index_tokens_cache = 0,
    use_text_index_negative_tokens_cache = 0,
    use_text_index_pattern_bypass_cache = 0,
    use_text_index_header_cache = 0,
    use_text_index_postings_cache = 0,
    use_query_condition_cache = 0,
    log_comment = '05394_text_index_page_cache_lookahead_default';

SYSTEM DROP PAGE CACHE;

-- Same search with `use_page_cache_lookahead_for_text_index = 1`: the normal lookahead, abandoned far more often.
SELECT count() FROM t_text_index_page_cache_lookahead
WHERE hasAnyTokens(s, ['kwneedle00', 'kwneedle05', 'kwneedle10', 'kwneedle15', 'kwneedle20', 'kwneedle25', 'kwneedle30', 'kwneedle35'])
SETTINGS
    enable_full_text_index = 1,
    use_page_cache_for_disks_without_file_cache = 1,
    page_cache_block_size = 1024,
    max_threads = 1,
    use_text_index_tokens_cache = 0,
    use_text_index_negative_tokens_cache = 0,
    use_text_index_pattern_bypass_cache = 0,
    use_text_index_header_cache = 0,
    use_text_index_postings_cache = 0,
    use_query_condition_cache = 0,
    use_page_cache_lookahead_for_text_index = 1,
    log_comment = '05394_text_index_page_cache_lookahead_setting_on';

SYSTEM DROP PAGE CACHE;

SELECT sum(n) FROM t_text_index_page_cache_lookahead_seq
SETTINGS
    use_page_cache_for_disks_without_file_cache = 1,
    page_cache_block_size = 1024,
    max_threads = 1,
    log_comment = '05394_text_index_page_cache_lookahead_seq';

SYSTEM FLUSH LOGS query_log;

-- Query A: many misses, but almost nothing is abandoned.
SELECT
    ProfileEvents['PageCacheMisses'] >= 100 AS default_many_misses,
    ProfileEvents['DiskConnectionsReset'] <= 2 AS default_few_resets
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment = '05394_text_index_page_cache_lookahead_default';

-- Query B: the setting clearly raises the number of abandoned connections.
SELECT ProfileEvents['DiskConnectionsReset'] > 2 AS setting_on_more_resets
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment = '05394_text_index_page_cache_lookahead_setting_on';

-- Query C: many misses, still grouped into few requests by the normal lookahead.
SELECT
    ProfileEvents['PageCacheMisses'] >= 100 AS seq_many_misses,
    ProfileEvents['S3GetObject'] * 8 < ProfileEvents['PageCacheMisses'] AS seq_few_requests
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment = '05394_text_index_page_cache_lookahead_seq';

DROP TABLE t_text_index_page_cache_lookahead;
DROP TABLE t_text_index_page_cache_lookahead_seq;

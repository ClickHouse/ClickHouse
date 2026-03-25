-- Tags: no-parallel, no-parallel-replicas, no-random-settings
-- Test: text_index_lazy_profile_events
-- Verifies that lazy posting list mode correctly increments ProfileEvents counters.

SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS t_text_idx_pe;

CREATE TABLE t_text_idx_pe
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', version = 2) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- Insert 100,000 rows with controlled token distribution.
INSERT INTO t_text_idx_pe
SELECT
    number,
    concat(
        'doc ', toString(number), ' ',
        if(number % 3 = 0, 'dense ', ''),
        if(number % 50 = 0, 'medium ', ''),
        if(number % 500 = 0, 'rare ', ''),
        if(number % 5000 = 0, 'ultrarare ', ''),
        'end'
    )
FROM numbers(100000);

OPTIMIZE TABLE t_text_idx_pe FINAL;

-- Sparse AND in lazy mode (should trigger leapfrog intersection)
SELECT count()
FROM t_text_idx_pe
WHERE hasAllTokens(text, ['dense', 'ultrarare'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         log_comment = 'lazy_sparse_and'
FORMAT Null;

-- Dense AND in lazy mode with low threshold (should trigger brute-force intersection)
SELECT count()
FROM t_text_idx_pe
WHERE hasAllTokens(text, ['dense', 'medium'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         text_index_density_threshold = 0.0,
         log_comment = 'lazy_dense_and_bruteforce'
FORMAT Null;

-- Same query in materialize mode (lazy counters should be zero)
SELECT count()
FROM t_text_idx_pe
WHERE hasAllTokens(text, ['dense', 'ultrarare'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         log_comment = 'materialize_sparse_and'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Verify lazy leapfrog intersection incremented counters
SELECT 'lazy_sparse_and';
SELECT
    ProfileEvents['TextIndexLazyLeapfrogIntersections'] > 0 AS has_leapfrog,
    ProfileEvents['TextIndexLazyPackedBlocksDecoded'] > 0 AS has_blocks_decoded,
    ProfileEvents['TextIndexLazySegmentsPrepared'] > 0 AS has_segments_prepared
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = 'lazy_sparse_and'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Verify lazy brute-force intersection incremented counters
SELECT 'lazy_dense_and_bruteforce';
SELECT
    ProfileEvents['TextIndexLazyBruteForceIntersections'] > 0 AS has_bruteforce,
    ProfileEvents['TextIndexLazyPackedBlocksDecoded'] > 0 AS has_blocks_decoded
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = 'lazy_dense_and_bruteforce'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Verify materialize mode does NOT increment lazy counters
SELECT 'materialize_no_lazy_events';
SELECT
    ProfileEvents['TextIndexLazyLeapfrogIntersections'] = 0 AS no_leapfrog,
    ProfileEvents['TextIndexLazyBruteForceIntersections'] = 0 AS no_bruteforce,
    ProfileEvents['TextIndexLazyPackedBlocksDecoded'] = 0 AS no_blocks_decoded,
    ProfileEvents['TextIndexLazySegmentsPrepared'] = 0 AS no_segments_prepared
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = 'materialize_sparse_and'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Verify sparse AND decoded fewer blocks than brute-force AND
SELECT 'sparse_fewer_blocks_than_bruteforce';
SELECT
    sparse.blocks < bruteforce.blocks AS sparse_decodes_fewer
FROM
(
    SELECT ProfileEvents['TextIndexLazyPackedBlocksDecoded'] AS blocks
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND log_comment = 'lazy_sparse_and'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
) AS sparse,
(
    SELECT ProfileEvents['TextIndexLazyPackedBlocksDecoded'] AS blocks
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND log_comment = 'lazy_dense_and_bruteforce'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
) AS bruteforce;

DROP TABLE t_text_idx_pe;

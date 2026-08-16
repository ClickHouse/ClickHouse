-- Regression test: `__text_index_*` virtual columns must be scheduled before physical-column
-- predicates in PREWHERE. They are filled by the index reader from posting lists (no column I/O),
-- but they have columns_size = 0, so the cost-per-rejected-row score used to fall back to
-- estimated_row_count (a row count) and lose against any byte-based score of a physical predicate.
-- The raw predicate then ran first, on all rows, reading the whole `text` column.
-- See https://github.com/ClickHouse/ClickHouse/pull/110695

SET allow_experimental_full_text_index = 1;
SET enable_analyzer = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1; -- CI may inject False, preventing statistics-based reordering
SET enable_multiple_prewhere_read_steps = 1; -- CI may inject False, collapsing PREWHERE into one step
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0; -- the second run must not skip reads via the cache
SET log_queries = 1;

DROP TABLE IF EXISTS t_prewhere_text_index_cost;

-- Wide parts so physical columns get real sizes and feed the byte-based cost score.
-- tdigest statistics on `v` make the estimator active (total_rows > 0), which routes
-- zero-size conditions into the score fallback where the defect lived.
CREATE TABLE t_prewhere_text_index_cost
(
    id UInt64,
    v UInt64 STATISTICS(tdigest),
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, auto_statistics_types = '';

-- Token `aaa` is in even rows, `bbb` in odd rows: every granule contains both tokens, so the
-- index prunes no granules and the filtering happens at row level, where the order matters.
-- No row contains both tokens, so the virtual columns reject everything and a correctly
-- ordered plan never reads `text` for the equality predicate. The per-row tail keeps the
-- `text` column's size nonzero so the equality predicate gets a byte-based cost score; the
-- hex alphabet (0-9A-F) cannot accidentally produce the lowercase query tokens. One hash per
-- row and no OPTIMIZE: the flaky check runs this test dozens of times on sanitizer builds
-- with S3 storage, under a 180 s cap.
SET max_insert_threads = 1; -- one part; a single INSERT is enough, no merge needed
INSERT INTO t_prewhere_text_index_cost
SELECT
    number,
    number % 1000,
    concat(if(number % 2 = 0, 'aaa', 'bbb'), ' row ', toString(number), ' ', repeat(hex(sipHash64(number)), 12))
FROM numbers(20000);

SELECT '-- correctness: no row has both tokens';

SELECT count() FROM t_prewhere_text_index_cost
WHERE hasToken(text, 'aaa') AND hasToken(text, 'bbb') AND text = concat('x', toString(v))
SETTINGS allow_reorder_prewhere_conditions = 0, log_comment = '04907_baseline_natural_order';

SELECT count() FROM t_prewhere_text_index_cost
WHERE hasToken(text, 'aaa') AND hasToken(text, 'bbb') AND text = concat('x', toString(v))
SETTINGS allow_reorder_prewhere_conditions = 1, log_comment = '04907_reordered';

SYSTEM FLUSH LOGS query_log;

-- With allow_reorder_prewhere_conditions = 0 the conditions keep their written order: the two
-- index virtual columns run first and reject all rows, so `text` is never read for the equality.
-- The reordered plan must not read more than that natural order (margin 2x for accounting noise).
SELECT '-- reordering must not schedule the raw predicate before the index virtual columns';
SELECT maxIf(read_bytes, log_comment = '04907_reordered') <= 2 * maxIf(read_bytes, log_comment = '04907_baseline_natural_order')
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('04907_baseline_natural_order', '04907_reordered');

DROP TABLE t_prewhere_text_index_cost;

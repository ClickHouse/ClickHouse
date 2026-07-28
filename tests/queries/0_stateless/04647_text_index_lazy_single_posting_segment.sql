-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel-replicas: the ProfileEvents guard below is read from the initiator's query_log.
-- Regression test for a wrong-results bug in the bitpacking posting list codec: a segment holding
-- exactly one posting is encoded with `bits == 0` (its only delta is 0), and the portable decoder
-- used on every non-x86_64-Linux build returned without writing the decoded zero. The lazy cursor
-- decodes into a reused buffer and then runs an in-place inclusive_scan over it, so the stale value
-- from the previous block turned into a bogus absolute row id and the posting was silently dropped.
--
-- The fixture is built so that all of the following hold; without any one of them the test passes
-- even on an unfixed build:
--   * `posting_list_codec` / `posting_list_block_size` are WRITE-TIME index properties, so they must
--     be given in the index definition. `posting_list_block_size = 64` rounds the segment capacity to
--     128 row ids, and 'bbb' has exactly 129 postings, so its last segment holds one posting.
--   * 'bbb' starts at row 1 (not row 0): the stale slot-0 value is the previous segment's first
--     absolute row id, so a series starting at 0 makes the wrong answer coincide with the right one.
--   * 'aaa' is absent at row 1026 (the bogus id 1025 + 1), otherwise the lost row is replaced by a
--     spurious match and `count()` alone is unchanged.
--   * `text_index_lazy_intersection_density_threshold = 1.0` forces the leapfrog intersection, which
--     is the only one reaching the block decoder; brute force takes the dense-segment shortcut.

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_lazy_single_posting_segment;

CREATE TABLE t_lazy_single_posting_segment
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', posting_list_block_size = 64) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;

INSERT INTO t_lazy_single_posting_segment
SELECT number, concat(if(number = 1026, 'zzz', 'aaa'), ' ', if(number % 8 = 1 AND number <= 1025, 'bbb', 'ccc'))
FROM numbers(4096);

-- `numbers(...)` is a single-stream source (`num_streams` is forced to 1), so the INSERT above
-- produces exactly one part regardless of the randomized `max_insert_threads`.
-- Fixture preconditions: a single part, and 'bbb' has 129 postings starting at row 1.
SELECT 'fixture', count(), min(id), max(id) FROM t_lazy_single_posting_segment WHERE hasToken(s, 'bbb') SETTINGS use_skip_indexes = 0;
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lazy_single_posting_segment' AND active;

-- Ground truth: the row-scan spelling of the same predicate, with no index involved.
SELECT 'rowscan', count(), sum(id) FROM t_lazy_single_posting_segment WHERE hasToken(s, 'aaa') AND hasToken(s, 'bbb') SETTINGS use_skip_indexes = 0;

-- FAILING REGIME (leapfrog). Both `use_skip_indexes_on_data_read` values are covered because the two
-- drifting blocks of 02346_text_index_hits differ only in that setting.
SELECT 'lazy leapfrog, on_data_read=1', count(), sum(id) FROM t_lazy_single_posting_segment
WHERE hasAllTokens(s, ['aaa', 'bbb'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         text_index_lazy_intersection_density_threshold = 1.0,
         query_plan_direct_read_from_text_index = 1,
         use_skip_indexes = 1,
         use_skip_indexes_on_data_read = 1,
         log_comment = 'test_04647_leapfrog_1';

SELECT 'lazy leapfrog, on_data_read=0', count(), sum(id) FROM t_lazy_single_posting_segment
WHERE hasAllTokens(s, ['aaa', 'bbb'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         text_index_lazy_intersection_density_threshold = 1.0,
         query_plan_direct_read_from_text_index = 1,
         use_skip_indexes = 1,
         use_skip_indexes_on_data_read = 0,
         log_comment = 'test_04647_leapfrog_0';

-- CONTROL: brute-force intersection takes the dense-segment shortcut and never decodes the
-- one-posting block, so it must be correct on every build, fixed or not.
SELECT 'lazy brute force (control)', count(), sum(id) FROM t_lazy_single_posting_segment
WHERE hasAllTokens(s, ['aaa', 'bbb'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         text_index_lazy_intersection_density_threshold = 0.0,
         query_plan_direct_read_from_text_index = 1,
         use_skip_indexes = 1,
         use_skip_indexes_on_data_read = 1,
         log_comment = 'test_04647_bruteforce';

-- CONTROL: the eager path builds a fresh decoder per segment, so it never sees a stale buffer.
SELECT 'materialize (control)', count(), sum(id) FROM t_lazy_single_posting_segment
WHERE hasAllTokens(s, ['aaa', 'bbb'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1,
         use_skip_indexes = 1;

-- Non-vacuity guard: prove from ProfileEvents that the two failing-regime statements really reached
-- the lazy leapfrog intersection and decoded packed blocks, and that the control took brute force.
-- A settings randomization that removes the direct-read path or the lazy mode makes this fail loudly
-- instead of leaving the assertions above trivially satisfied.
SYSTEM FLUSH LOGS query_log;

-- The time bound below is the in-tree convention, not a correctness requirement: it does NOT close
-- the stress-regime residual where odd workers share `--database=test_{i}` and the retry loop re-runs
-- this script against the same database without cleanup, so a retried run can still see both
-- attempts' rows. Closing that would need a per-run discriminator inside `log_comment`.
SELECT
    replaceOne(log_comment, 'test_04647_', '') AS which,
    ProfileEvents['TextIndexLazyLeapfrogIntersections'] > 0 AS used_leapfrog,
    ProfileEvents['TextIndexLazyBruteForceIntersections'] > 0 AS used_brute_force,
    ProfileEvents['TextIndexLazyPackedBlocksDecoded'] > 0 AS decoded_packed_blocks
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND log_comment LIKE 'test_04647_%'
  AND type = 'QueryFinish'
ORDER BY which
SETTINGS max_rows_to_read = 0;

DROP TABLE t_lazy_single_posting_segment;

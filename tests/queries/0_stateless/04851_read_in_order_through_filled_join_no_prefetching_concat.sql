-- Tags: no-random-settings, no-random-merge-tree-settings
-- When the read order is preserved through a filled join (`FilledJoinStep`, e.g. a right-hand
-- `Join`-engine table), the join is a simple per-stream transform: it keeps exactly the
-- probe-side streams it receives and runs one `JoiningTransform` per stream.
-- `PrefetchingConcatProcessor` must NOT collapse a single-part filtered read into one stream
-- there: it would serialize all the per-stream join work. Expect no PrefetchingConcat and a
-- multi-stream join probe side. This complements
-- 04848_read_in_order_through_join_no_prefetching_concat, which covers `JoinStep`.

-- Same determinism preamble as 04054_read_in_order_prefetching_concat.
SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_aggregation_in_order = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET parallel_replicas_local_plan = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;

-- Settings enabling read-in-order through JOIN (as in 04657).
SET enable_analyzer = 1;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET read_in_order_use_virtual_row = 1;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET query_plan_optimize_join_order_limit = 1, query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS events_04851;
DROP TABLE IF EXISTS payloads_04851;

CREATE TABLE events_04851 (path String, id UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number % 40 FROM numbers(1000000);

OPTIMIZE TABLE events_04851 FINAL;

-- A `Join`-engine right-hand table makes the planner use `FilledJoinStep` instead of `JoinStep`.
CREATE TABLE payloads_04851 (id UInt64, payload String) ENGINE = Join(ANY, LEFT, id)
AS SELECT number, concat('Payload ', toString(number)) FROM numbers(40) WHERE number % 4 = 0;

-- Positive (vacuity) control: without the JOIN, the same filtered single-part in-order read
-- does use PrefetchingConcat.
SELECT 'has_prefetching_concat_no_join';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT path, id FROM events_04851
    WHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%';

-- The join is a `FilledJoinStep`, not a `JoinStep` (otherwise this test is vacuous vs 04848).
SELECT 'is_filled_join';
SELECT count() > 0 FROM (
    EXPLAIN PLAN SELECT e.path, p.payload
    FROM events_04851 AS e
    LEFT ANY JOIN payloads_04851 AS p ON e.id = p.id
    WHERE e.path LIKE '%file.log'
    ORDER BY e.path
) WHERE explain LIKE '%FilledJoin%';

-- Through the filled JOIN: no PrefetchingConcat.
SELECT 'no_prefetching_concat_through_filled_join';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT e.path, p.payload
    FROM events_04851 AS e
    LEFT ANY JOIN payloads_04851 AS p ON e.id = p.id
    WHERE e.path LIKE '%file.log'
    ORDER BY e.path
) WHERE explain LIKE '%PrefetchingConcat%';

-- The join probe side stays multi-stream: a parallel `JoiningTransform` renders as
-- `JoiningTransform × N` in `EXPLAIN PIPELINE` (a single-stream one has no multiplier).
SELECT 'join_probe_multi_stream';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT e.path, p.payload
    FROM events_04851 AS e
    LEFT ANY JOIN payloads_04851 AS p ON e.id = p.id
    WHERE e.path LIKE '%file.log'
    ORDER BY e.path
) WHERE explain LIKE '%JoiningTransform × %';

-- Vacuity control for the two checks above: the sort above the join is still applied in-order
-- (a full sort would show up as `MergeSortingTransform`, making the no-PrefetchingConcat result
-- trivial).
SELECT 'no_full_sort_through_filled_join';
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT e.path, p.payload
    FROM events_04851 AS e
    LEFT ANY JOIN payloads_04851 AS p ON e.id = p.id
    WHERE e.path LIKE '%file.log'
    ORDER BY e.path
) WHERE explain LIKE '%MergeSortingTransform%';

-- Correctness: output must be sorted.
SELECT 'correctness';
SELECT count(), countIf(path < prev_path) AS violations FROM (
    SELECT path, lagInFrame(path, 1, '') OVER (ORDER BY rowNumberInAllBlocks()) AS prev_path
    FROM (
        SELECT e.path AS path, p.payload
        FROM events_04851 AS e
        LEFT ANY JOIN payloads_04851 AS p ON e.id = p.id
        WHERE e.path LIKE '%file.log'
        ORDER BY e.path
    )
);

DROP TABLE payloads_04851;
DROP TABLE events_04851;

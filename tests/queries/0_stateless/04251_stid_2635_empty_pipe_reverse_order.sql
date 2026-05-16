-- Regression test for STID 2635-3a9e: `Logical error: 'Cannot add simple transform to empty Pipe.'`
--
-- `ReadFromMergeTree::readInOrder` builds one `Pipe` per part in `parts_with_ranges` and unites them.
-- When `parts_with_ranges` is empty (legitimate cases include an empty layer from
-- `splitIntersectingPartsRangesIntoLayers`, or the non-intersecting branch of
-- `splitPartsWithRangesByPrimaryKey` having no parts) `Pipe::unitePipes` returns an empty
-- `Pipe` with no output ports. For `ReadType::InReverseOrder` the function then unconditionally
-- added a `ReverseTransform` via `Pipe::addSimpleTransform`, which throws `LOGICAL_ERROR`.
--
-- The fix in `src/Processors/QueryPlan/ReadFromMergeTree.cpp` guards the `addSimpleTransform`
-- call with `pipe.numOutputPorts() > 0`. Reversing zero rows is a no-op, and callers already
-- handle empty pipes (e.g. `ReadFromMergeTree::readByLayers` substitutes a `NullSource`).
--
-- This test exercises the `read-in-order-through-join` + `read-by-layers` + `ORDER BY ... DESC`
-- path under stress settings (full split injection, two-level merge threshold = 1, high thread
-- count) to keep coverage of the previously crashing path, and verifies that the previously
-- broken `Pipe::unitePipes` -> `addSimpleTransform` sequence no longer aborts when an empty
-- pipe is produced.
--
-- The test asserts only that each shape returns without a logical error (we wrap each result
-- in `count()` so that the expected output is deterministic regardless of randomized
-- `index_granularity`, `max_threads`, layer-count, etc.).
--
-- Reported via server-side AST fuzzer (`serverfuzz`) on master, 2026-05-15:
--   STID 2635-3a9e, amd_tsan + arm_debug, source test 04230_top_k_through_join_join_swap_gate.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS stid_2635_l;
DROP TABLE IF EXISTS stid_2635_r;

CREATE TABLE stid_2635_l (k Int64, payload String) ENGINE = MergeTree() ORDER BY k
SETTINGS index_granularity = 8, parts_to_delay_insert = 10000;

CREATE TABLE stid_2635_r (k Int64, value String) ENGINE = MergeTree() ORDER BY k
SETTINGS index_granularity = 8;

SYSTEM STOP MERGES stid_2635_l;

-- Many small, highly-overlapping parts maximise the chance of producing an empty layer
-- via `splitIntersectingPartsRangesIntoLayers`.
INSERT INTO stid_2635_l SELECT number, 'a' FROM numbers(8);
INSERT INTO stid_2635_l SELECT number, 'b' FROM numbers(8);
INSERT INTO stid_2635_l SELECT number, 'c' FROM numbers(8);
INSERT INTO stid_2635_l SELECT number, 'd' FROM numbers(8);
INSERT INTO stid_2635_l SELECT number, 'e' FROM numbers(8);
INSERT INTO stid_2635_l SELECT number, 'f' FROM numbers(8);
INSERT INTO stid_2635_l SELECT number, 'g' FROM numbers(8);
INSERT INTO stid_2635_l SELECT number, 'h' FROM numbers(8);

INSERT INTO stid_2635_r SELECT number, 'r' FROM numbers(8);

-- Case 1: single-table `ORDER BY ... DESC LIMIT` with forced intersecting-layer split.
--         Goes through `spreadMarkRangesAmongStreamsWithOrder` -> `readByLayers`
--         -> `read(... InReverseOrder)`. Must not abort.
SELECT 'case1', count() FROM (
    SELECT k FROM stid_2635_l
    ORDER BY k DESC
    LIMIT 3
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1,
             merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1.0,
             use_query_condition_cache = 0,
             read_in_order_two_level_merge_threshold = 1,
             max_threads = 8
);

-- Case 2: empty table on the read-in-order side. The MergeTree read pipeline produces an
--         empty pipe by design; the reverse-order path used to abort instead of returning
--         the empty pipe.
DROP TABLE stid_2635_l;
CREATE TABLE stid_2635_l (k Int64, payload String) ENGINE = MergeTree() ORDER BY k
SETTINGS index_granularity = 8;

SELECT 'case2_empty', count() FROM (
    SELECT k FROM stid_2635_l
    ORDER BY k DESC
    LIMIT 3
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, max_threads = 4
);

-- Case 3: full `LEFT JOIN` + `read-in-order-through-join` + `ORDER BY DESC` shape from the
--         original 04230_top_k_through_join_join_swap_gate.sql, with the stress knobs that
--         exercise the `readByLayers` -> `InReverseOrder` lambda path.
SYSTEM STOP MERGES stid_2635_l;
INSERT INTO stid_2635_l SELECT number, 'a' FROM numbers(8);
INSERT INTO stid_2635_l SELECT number, 'b' FROM numbers(8);
INSERT INTO stid_2635_l SELECT number, 'c' FROM numbers(8);
INSERT INTO stid_2635_l SELECT number, 'd' FROM numbers(8);

SELECT 'case3', count() FROM (
    SELECT l.k FROM stid_2635_l AS l LEFT JOIN stid_2635_r AS r ON r.k = l.k
    ORDER BY l.k DESC NULLS LAST
    LIMIT 3
    SETTINGS query_plan_join_swap_table = false,
             optimize_read_in_order = 1,
             query_plan_read_in_order = 1,
             query_plan_read_in_order_through_join = 1,
             query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0,
             enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0,
             max_bytes_ratio_before_external_join = 0,
             merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1.0,
             use_query_condition_cache = 0,
             read_in_order_two_level_merge_threshold = 1,
             max_threads = 8
);

-- Case 4: build the inner-query plan via `EXPLAIN PIPELINE` (the same path that the
--         `viewExplain(...)` table function takes at plan time). We assert only that the
--         plan-builder produced at least one line of output without aborting, so the
--         assertion is stable across builds, thread counts, and future plan refactors.
SELECT 'case4_explain_nonempty', count() > 0 FROM (
    EXPLAIN PIPELINE
    SELECT l.k FROM stid_2635_l AS l LEFT JOIN stid_2635_r AS r ON r.k = l.k
    ORDER BY l.k DESC NULLS LAST
    LIMIT 3
    SETTINGS query_plan_join_swap_table = false,
             optimize_read_in_order = 1,
             query_plan_read_in_order = 1,
             query_plan_read_in_order_through_join = 1,
             enable_parallel_replicas = 0,
             merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1.0,
             use_query_condition_cache = 0,
             read_in_order_two_level_merge_threshold = 1,
             max_threads = 8
) WHERE explain != '';

SELECT 'done';

DROP TABLE stid_2635_l;
DROP TABLE stid_2635_r;

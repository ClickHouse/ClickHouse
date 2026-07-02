-- Regression test for STID 2635-3a9e: `Logical error: 'Cannot add simple transform to empty Pipe.'`
--
-- `ReadFromMergeTree::readInOrder` builds one `Pipe` per part in `parts_with_ranges` and unites
-- them, then for `ReadType::InReverseOrder` added a `ReverseTransform` via
-- `Pipe::addSimpleTransform`. When `parts_with_ranges` is empty `Pipe::unitePipes` returns an
-- empty `Pipe` with no output ports and `addSimpleTransform` threw `LOGICAL_ERROR`.
--
-- The reachable empty-`parts_with_ranges` case is the per-source empty layer produced by join
-- sharding (`optimizeJoinByShards`, gated by `query_plan_join_shard_by_pk_ranges`): the layers
-- are built once over the union of both join sides' parts and then redistributed per source, so
-- a PK band that only contains parts from the other side leaves an empty layer for this source.
-- With a reverse primary key that layer is read via `read({}, ReadType::InReverseOrder)`.
--
-- The fix guards the `addSimpleTransform` call with `pipe.numOutputPorts() > 0`. Reversing zero
-- rows is a no-op, and callers already handle empty pipes (`readByLayers` substitutes a
-- `NullSource`). Without the guard the SHARD_JOIN case below aborts; with it the query succeeds.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS stid_2635_l;
DROP TABLE IF EXISTS stid_2635_r;

-- Reverse primary key so the read goes through `ReadType::InReverseOrder`; `index_granularity = 1`
-- maximises the number of PK-range layers.
CREATE TABLE stid_2635_l (k Int) ENGINE = MergeTree() ORDER BY (k DESC)
SETTINGS index_granularity = 1, allow_experimental_reverse_key = 1;

CREATE TABLE stid_2635_r (k Int) ENGINE = MergeTree() ORDER BY (k DESC)
SETTINGS index_granularity = 1, allow_experimental_reverse_key = 1;

-- Left covers PK band [0, 50). Right covers the same [0, 50) plus a disjoint high band
-- [1000, 1200). The disjoint high band yields a global layer with only right-side parts, hence
-- an empty layer for the left (reverse-order) source, which is the regressed read path.
INSERT INTO stid_2635_l SELECT number FROM numbers(50);
INSERT INTO stid_2635_r SELECT number FROM numbers(50);
INSERT INTO stid_2635_r SELECT number + 1000 FROM numbers(200);

-- This query reaches `readByLayers` -> `read({}, ReadType::InReverseOrder)` for the empty left
-- layer. It aborted with the empty-pipe LOGICAL_ERROR before the fix. `FORMAT Null` is required:
-- wrapping the result in an aggregate prunes the `ORDER BY` and the read-by-layers path with it.
-- `optimize_read_in_order` and `query_plan_read_in_order` are pinned because the test runner
-- randomizes the former: with either off, `read_in_order` is not set and the guarded path is skipped.
SELECT k FROM stid_2635_l JOIN stid_2635_r USING (k)
ORDER BY k DESC
SETTINGS join_algorithm = 'full_sorting_merge',
         query_plan_join_shard_by_pk_ranges = 1,
         optimize_read_in_order = 1,
         query_plan_read_in_order = 1,
         query_plan_read_in_order_through_join = 1,
         read_in_order_use_virtual_row = 1,
         enable_join_runtime_filters = 0,
         query_plan_join_swap_table = false,
         max_threads = 8
FORMAT Null;

-- The query above ran without aborting; the result itself is checked here (50 matching keys).
SELECT 'shard_join_reverse', count()
FROM (SELECT k FROM stid_2635_l JOIN stid_2635_r USING (k));

-- Empty table on the reverse read-in-order side: the read pipeline legitimately produces an
-- empty pipe, which the reverse-order path must pass through rather than abort.
DROP TABLE stid_2635_l;
CREATE TABLE stid_2635_l (k Int) ENGINE = MergeTree() ORDER BY (k DESC)
SETTINGS index_granularity = 1, allow_experimental_reverse_key = 1;

SELECT 'empty_reverse', count() FROM (
    SELECT k FROM stid_2635_l
    ORDER BY k DESC
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, max_threads = 4
);

DROP TABLE stid_2635_l;
DROP TABLE stid_2635_r;

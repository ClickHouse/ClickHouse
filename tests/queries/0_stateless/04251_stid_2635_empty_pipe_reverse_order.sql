-- Regression test for STID 2635-3a9e: `Logical error: 'Cannot add simple transform to empty Pipe.'`
--
-- `ReadFromMergeTree::readInOrder` builds one `Pipe` per part in `parts_with_ranges`, unites them,
-- and for `ReadType::InReverseOrder` adds a `ReverseTransform` via `Pipe::addSimpleTransform`. When
-- `parts_with_ranges` is empty, `Pipe::unitePipes` returns an empty `Pipe` (no output ports) and
-- `addSimpleTransform` threw the LOGICAL_ERROR. Master fixed it in `readInOrder` with an early
-- `if (pipe.empty()) return pipe;` right after `unitePipes` (commit e45eb196113).
--
-- The reachable empty-`parts_with_ranges` case is the per-source empty layer produced by join
-- sharding (`optimizeJoinByShards`, gated by `query_plan_join_shard_by_pk_ranges`): the layers are
-- built once over the union of both join sides' parts and then redistributed per source, so a PK
-- band that only contains parts from the other side leaves an empty layer for this source. With a
-- reverse primary key that layer is read via `read({}, ReadType::InReverseOrder)`. Without the guard
-- this query aborts; with it, it returns the correct rows.
--
-- The ordered output of the exact guarded query is asserted directly (the 50 shared keys, DESC), not
-- via an order-independent aggregate such as `count()`. Two reasons: the top-level `ORDER BY k DESC`
-- must survive so the read stays on the reverse-order path (an aggregate wrapper would let
-- `RemoveRedundantSorting` drop the sort and the plan would read in `ReadType::Default`), and checking
-- the rows themselves catches a regression that silently loses or duplicates rows through
-- `read({}, ReadType::InReverseOrder)`, not just one that throws. `optimize_read_in_order` and
-- `query_plan_read_in_order` are pinned because the test runner randomizes the former: with either
-- off, `read_in_order` is not set and the guarded path is skipped.

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
-- layer. It aborted with the empty-pipe LOGICAL_ERROR before the guard. Its ordered output (the 50
-- shared keys, descending) is checked directly.
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
         max_threads = 8;

DROP TABLE stid_2635_l;
DROP TABLE stid_2635_r;

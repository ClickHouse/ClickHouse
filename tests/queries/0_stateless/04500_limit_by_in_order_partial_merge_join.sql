-- LIMIT BY is the third consumer of the preservesLeftBlockOrder() contract, alongside
-- optimize_distinct_in_order (04498) and optimize_aggregation_in_order (04498). buildInputOrderInfo
-- for a LimitByStep follows the same findReadingStep path in optimizeReadInOrder.cpp, so read-in-order
-- must not be propagated through a join that reorders the left block; otherwise LimitBySortedStreamTransform
-- gets a stream where equal grouping-key values are no longer contiguous and hits the same failure class
-- as the DISTINCT case (wrong results in release, the "Equal values are not contiguous within the range
-- assumed to be sorted" LOGICAL_ERROR in debug via getEqualRangeEndAssumeSorted).
--
-- The left table is ordered by (a, j) so a MergeTree read returns rows in a-order. The join is on j,
-- a different column, so partial_merge / prefer_partial_merge / full_sorting_merge re-order the left
-- rows by j; equal a values stop being contiguous. The read-in-order-through-join second pass must
-- refuse to propagate the a-order through these joins. It reads the physical join's
-- preservesLeftBlockOrder() (MergeJoin / FullSortingMergeJoin return false), so this holds under both
-- the old and the current analyzer; enable_analyzer is intentionally not pinned.
--
-- Note this file is a coverage guard for the LIMIT BY consumer, not a fail-without-the-fix test:
-- MergeJoin already reported false before this change and full_sorting_merge is blocked by its own
-- pre-JOIN SortingStep, so every assertion below also holds without it. The assertions that do move
-- with this change are the parallel_hash ones in 04498 and the constant-join one in
-- 04500_read_in_order_through_constant_join.

DROP TABLE IF EXISTS tl_04500;
DROP TABLE IF EXISTS tr_04500;

CREATE TABLE tl_04500 (a UInt32, j UInt64) ENGINE = MergeTree ORDER BY (a, j);
CREATE TABLE tr_04500 (j UInt64, v UInt64) ENGINE = MergeTree ORDER BY j;

-- Freeze merges so the two right-side inserts stay as two separate parts. partial_merge re-scans the
-- left key ranges once per right block, so the reorder only shows up with more than one right block;
-- a background merge collapsing them would let it read a single block and pass spuriously.
SYSTEM STOP MERGES tr_04500;

-- 8 groups a = 0..7, each with 8 rows j = 0..7 (one-to-one on j). LEFT ALL JOIN keeps every left row,
-- so the join output is 64 rows in 8 groups of 8.
INSERT INTO tl_04500 SELECT intDiv(number, 8)::UInt32, (number % 8)::UInt64 FROM numbers(64);
INSERT INTO tr_04500 SELECT (number % 8)::UInt64, number FROM numbers(4);
INSERT INTO tr_04500 SELECT (number % 8)::UInt64 + 4, number + 4 FROM numbers(4);

-- LIMIT 3 BY a keeps 3 rows per group: 8 * 3 = 24 rows. If read-in-order were wrongly propagated
-- through the reordering join, LimitBySortedStreamTransform would receive a non-contiguous a stream
-- and close each group early (wrong count) or hit the sort assertion.
--
-- Pin the whole read-in-order trio on every query (the stateless runner randomizes all three): with
-- query_plan_read_in_order = 0 the in-order LimitBy consumer is never planted, so the query is correct
-- even on an unfixed binary and the guard goes blind. Spilling is pinned off and join_swap_table off so
-- preservesLeftBlockOrder() is the only remaining gate. Parallel replicas is pinned off too: it is a
-- plan-affecting setting the ParallelReplicas CI variant enables in the default profile, under which
-- the MergeTree read is remote and the local read-in-order LimitBy plan does not apply (the InOrder
-- plan probe would then read 0 instead of the expected 1).

SELECT count() FROM (
    SELECT a FROM tl_04500 LEFT ALL JOIN tr_04500 ON tl_04500.j = tr_04500.j
    LIMIT 3 BY a
    SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT count() FROM (
    SELECT a FROM tl_04500 LEFT ALL JOIN tr_04500 ON tl_04500.j = tr_04500.j
    LIMIT 3 BY a
    SETTINGS join_algorithm = 'prefer_partial_merge', query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT count() FROM (
    SELECT a FROM tl_04500 LEFT ALL JOIN tr_04500 ON tl_04500.j = tr_04500.j
    LIMIT 3 BY a
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- Plan-level guard: the read-in-order-through-join propagation must NOT reach the MergeTree read for
-- the reordering partial_merge join, so no left read is done InOrder (assertion result 0). Before the
-- fix this returned 1 and fed LimitBySortedStreamTransform a non-contiguous stream.
SELECT count() FROM (
    EXPLAIN PLAN
    SELECT a FROM tl_04500 LEFT ALL JOIN tr_04500 ON tl_04500.j = tr_04500.j
    LIMIT 3 BY a
    SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain ILIKE '%Read type: InOrder%';

-- Order-preserving hash must be unchanged: it legitimately propagates read-in-order through the join,
-- so LimitByStreamTransform runs in the efficient streaming (InOrder) mode. The left read IS done
-- InOrder (assertion result > 0) and the count is still correct (24). This proves the gate is precise
-- (keyed on order preservation), not a blanket disable of LIMIT-BY-in-order through every join.
SELECT count() FROM (
    SELECT a FROM tl_04500 LEFT ALL JOIN tr_04500 ON tl_04500.j = tr_04500.j
    LIMIT 3 BY a
    SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT count() > 0 FROM (
    EXPLAIN PLAN
    SELECT a FROM tl_04500 LEFT ALL JOIN tr_04500 ON tl_04500.j = tr_04500.j
    LIMIT 3 BY a
    SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain ILIKE '%Read type: InOrder%';

DROP TABLE tl_04500;
DROP TABLE tr_04500;

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
-- Note the partial_merge / prefer_partial_merge / full_sorting_merge / hash cases below are
-- coverage guards rather than fail-without-the-fix assertions: MergeJoin already reported false
-- before this change and full_sorting_merge is blocked by its own pre-JOIN SortingStep. The
-- parallel_hash case at the end of this file is the one whose classification this change moves,
-- and it does return wrong results without it.

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

-- parallel_hash with a single-level (key8) map is the shape whose classification this change
-- actually moves, and LIMIT BY is affected exactly like DISTINCT and aggregation are: the join
-- scatters the left block across slots and emits slot 0, then 1, ..., so equal a values stop
-- being contiguous and LimitBySortedStreamTransform reopens a group it has already closed.
-- 8 distinct a values, one row each; without the fix every a survives 8 times, so 64 rows.
DROP TABLE IF EXISTS tl2_04500;
DROP TABLE IF EXISTS tr2_04500;
CREATE TABLE tl2_04500 (a UInt32, j UInt8) ENGINE = MergeTree ORDER BY (a, j);
CREATE TABLE tr2_04500 (j UInt8, v UInt64) ENGINE = MergeTree ORDER BY j;
INSERT INTO tl2_04500 SELECT intDiv(number, 8)::UInt32, (number % 8)::UInt8 FROM numbers(64);
INSERT INTO tr2_04500 SELECT (number % 8)::UInt8, number FROM numbers(8);

SELECT count() FROM (
    SELECT a FROM tl2_04500 LEFT ALL JOIN tr2_04500 ON tl2_04500.j = tr2_04500.j
    ORDER BY a LIMIT 1 BY a
    SETTINGS join_algorithm = 'parallel_hash', max_threads = 8, query_plan_join_swap_table = 0,
        enable_parallel_replicas = 0, optimize_read_in_order = 1, query_plan_read_in_order = 1,
        query_plan_read_in_order_through_join = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

DROP TABLE tl2_04500;
DROP TABLE tr2_04500;

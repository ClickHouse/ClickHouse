-- topKThroughJoin must still push `Sort + Limit` below the join for join algorithms that
-- do NOT preserve the left block order (partial_merge / prefer_partial_merge / full_sorting_merge).
--
-- Those algorithms return `preservesLeftBlockOrder() == false`, so the read-in-order-through-join
-- second pass (findReadingStep in optimizeReadInOrder.cpp) refuses to propagate the left sort
-- order through the join. Before the fix, topKThroughJoin's pass-1 deferral only checked for
-- delayed blocks, so it still deferred to that second pass; the second pass then bailed and
-- NEITHER optimization fired, leaving a full join plus a full sort. The deferral now also
-- requires preservesLeftBlockOrder(), so the pushdown runs in pass 1 for these algorithms.
--
-- All plan-affecting settings are pinned in each query's SETTINGS clause (query-level settings
-- take precedence over the CI settings randomizer): read-in-order enabled so the deferral path
-- is live; spilling off so joinMayHaveDelayedBlocks is false; join side pinned so the deferral
-- is otherwise satisfiable and preservesLeftBlockOrder() is the only remaining gate.
--
-- The join is one-to-one (each left key matches exactly one right row), so the correctness probes
-- return the exact top-10 pk 0..9. That makes the pushed limit's value and side observable: a
-- pushdown that keeps too few rows (for example a wrong `Limit 1`) or attaches the limit to the
-- wrong side changes the returned rows, instead of hiding behind a fan-out of equal pk. The
-- EXPLAIN probes assert the pushed `Limit` step is present; the correctness probes below pin its
-- value and side.

DROP TABLE IF EXISTS tl_04499;
DROP TABLE IF EXISTS tr_04499;

CREATE TABLE tl_04499 (pk UInt64, j UInt64) ENGINE = MergeTree ORDER BY pk
    AS SELECT number, number FROM numbers(100000);
CREATE TABLE tr_04499 (j UInt64, v UInt64) ENGINE = MergeTree ORDER BY j
    AS SELECT number, number FROM numbers(100000);

-- topKThroughJoin grafts a `Limit` step onto the preserved (left) side. The `%Limit` (plain
-- step header, excluding the `Limit (preliminary LIMIT)` header) is present only when the
-- pushdown fired. It must fire for every non-order-preserving algorithm.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'partial_merge',
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'prefer_partial_merge',
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'full_sorting_merge',
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%';

-- parallel_hash (ConcurrentHashJoin) does not preserve the left order for single-level map key
-- shapes (key8 / key16 / single non-nullable LowCardinality), and the map type is unknown at the
-- logical stage where this deferral runs. So parallel_hash is treated conservatively as
-- non-order-preserving too: the pushdown must fire in pass 1 instead of deferring to a pass-2
-- read-in-order path that the physical gate can refuse.
-- enable_analyzer is pinned to 1 here: this deferral runs on the logical JoinStepLogical, which
-- only exists in the new analyzer. Under the old analyzer topKThroughJoin sees the already-built
-- physical ConcurrentHashJoin and reads its exact preservesLeftBlockOrder() (true for the
-- two-level UInt64 map used here), so it correctly defers and read-in-order fires instead - a
-- different, also-correct plan that would flip this assertion to 0. Pinned on both the wrapper
-- and the EXPLAIN because the old-analyzer job forbids changing enable_analyzer in a subquery.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

-- The default `direct,parallel_hash,hash` list has a `hash` fallback, but `PlannerJoins::tryCreateJoin`
-- still picks `ConcurrentHashJoin` when there is no right-side estimate or the right side is at least
-- `parallel_hash_join_threshold` - and on a single-level map key that scatters the left order. The map
-- type and the right-side size are unknown at this logical stage, so the deferral must treat the list
-- as possibly non-order-preserving and fire the pushdown; otherwise a large-RHS single-level join would
-- defer to a second pass that then bails on the physical `!preservesLeftBlockOrder()` gate, losing both.
-- enable_analyzer pinned to 1 for the same reason as the parallel_hash probe above: the list is only
-- treated as possibly-non-order-preserving on the logical JoinStepLogical (new analyzer); under the old
-- analyzer the physical join is inspected directly and correctly defers, flipping this assertion to 0.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'direct,parallel_hash,hash', enable_analyzer = 1,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

-- Order-preserving `hash` must be unchanged: it legitimately defers to read-in-order-through-join
-- (which keeps the left primary-key order across the join), so there is no explicit pushed Limit.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'hash',
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%';

-- Correctness pins the pushed limit's value and side. Because the join is one-to-one, the top-10
-- pk are exactly 0..9: a pushdown that keeps only the first row (a wrong `Limit 1`) would return a
-- single 0, and a limit pushed onto the right side would drop or reorder left rows. This is the
-- one-to-one form the earlier fan-out data could not prove: ten equal pk = 0 hid a wrong limit.
SELECT tl_04499.pk
FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
ORDER BY tl_04499.pk LIMIT 10
SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SELECT tl_04499.pk
FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
ORDER BY tl_04499.pk LIMIT 10
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

DROP TABLE tl_04499;
DROP TABLE tr_04499;

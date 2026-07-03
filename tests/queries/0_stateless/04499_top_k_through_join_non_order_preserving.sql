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
DROP TABLE IF EXISTS tl8_04499;
DROP TABLE IF EXISTS tr8_04499;

CREATE TABLE tl_04499 (pk UInt64, j UInt64) ENGINE = MergeTree ORDER BY pk
    AS SELECT number, number FROM numbers(100000);
CREATE TABLE tr_04499 (j UInt64, v UInt64) ENGINE = MergeTree ORDER BY j
    AS SELECT number, number FROM numbers(100000);

-- key8 join key (UInt8): its ConcurrentHashJoin builds a single-level `key8` map (the two-level
-- map switch in HashJoin::chooseMethod leaves key8/key16 single-level), which scatters the left
-- block across slots, so multi-slot parallel_hash on this shape does NOT preserve the left order.
-- Used to prove the key-shape split: multi-slot parallel_hash still pushes down here.
CREATE TABLE tl8_04499 (pk UInt64, j UInt8) ENGINE = MergeTree ORDER BY pk
    AS SELECT number, toUInt8(number % 256) FROM numbers(100000);
CREATE TABLE tr8_04499 (j UInt8, v UInt64) ENGINE = MergeTree ORDER BY j
    AS SELECT toUInt8(number % 256), number FROM numbers(100000);

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

-- parallel_hash (ConcurrentHashJoin) preserves the left order only for two-level maps; it scatters
-- it for single-level map key shapes (key8 / key16: a single numeric key of at most 2 bytes). The
-- logical deferral mirrors that key-shape split (see parallelHashMultiSlotSingleLevelMap):
--
--  - Two-level key (UInt64 here): multi-slot parallel_hash keeps the order, so this DEFERS to the
--    read-in-order-through-join second pass. First probe: NO explicit pushed Limit on the left
--    (count = 0). Before the key-shape split every multi-slot parallel_hash was flagged and this
--    returned 1, forcing the pushdown and losing the cheaper through-join plan. Second probe: the
--    left MergeTree read streams InOrder (count > 0), confirming read-in-order really fired.
--  - Single-level key (UInt8 / key8, below): multi-slot parallel_hash reorders, so the pushdown
--    must still fire in pass 1 (the pass-2 physical gate would otherwise refuse and lose both).
--
-- max_threads is pinned above 1 so the join has several slots (single-slot is covered separately
-- below). enable_analyzer is pinned to 1: this deferral runs on the logical JoinStepLogical, which
-- only exists in the new analyzer; under the old analyzer topKThroughJoin sees the already-built
-- physical ConcurrentHashJoin and reads its exact preservesLeftBlockOrder(), a different (also
-- correct) path. The stateless runner randomizes max_threads and the in-order trio, so both are
-- pinned per query. Pinned on both the wrapper and the EXPLAIN because the old-analyzer job forbids
-- changing enable_analyzer in a subquery.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain ILIKE '%Read type: InOrder%'
SETTINGS enable_analyzer = 1;

-- Single-level key8 (UInt8): multi-slot parallel_hash scatters the left block, so the pushdown must
-- still fire (count > 0). This is the key-shape split's other side: without it, a two-level-only gate
-- would defer here and the pass-2 physical gate would then bail on !preservesLeftBlockOrder(), losing
-- both optimizations.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl8_04499.pk, tr8_04499.v
    FROM tl8_04499 LEFT ALL JOIN tr8_04499 ON tl8_04499.j = tr8_04499.j
    ORDER BY tl8_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

-- The default `direct,parallel_hash,hash` list on a single-level key8: `PlannerJoins::tryCreateJoin`
-- can still pick `ConcurrentHashJoin` (no right-side estimate or right side at least
-- `parallel_hash_join_threshold`) even with the hash fallback, and on key8 that scatters the left
-- order. The right-side size is unknown at this logical stage, so the deferral must treat a
-- single-level parallel_hash in the list as non-order-preserving and fire the pushdown.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl8_04499.pk, tr8_04499.v
    FROM tl8_04499 LEFT ALL JOIN tr8_04499 ON tl8_04499.j = tr8_04499.j
    ORDER BY tl8_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'direct,parallel_hash,hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

-- Single-slot parallel_hash (max_threads = 1) preserves the left order: dispatchBlock short-circuits
-- on one slot and passes the left block through unscattered regardless of the map type, so
-- ConcurrentHashJoin::preservesLeftBlockOrder() is true. The logical topKThroughJoin deferral mirrors
-- that (max_threads == 1 -> not flagged as reordering), so it defers to the read-in-order-through-join
-- second pass instead of injecting a Sort + Limit.
-- First probe is the guard: with the fix there is NO explicit pushed Limit on the left (count = 0);
-- before threading max_threads into the logical check the pushdown fired here and this returned 1.
-- Second probe is an invariant: the left MergeTree read must stream InOrder (count > 0), confirming
-- the recovered plan really reads in primary-key order through the join. max_threads = 1 and the
-- in-order trio are pinned per-query (the stateless runner randomizes both).
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 1,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 1,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain ILIKE '%Read type: InOrder%'
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
DROP TABLE tl8_04499;
DROP TABLE tr8_04499;

-- topKThroughJoin must still push `Sort + Limit` below the join for join algorithms that do NOT
-- preserve the left block order. The pass-1 deferral proves order preservation only for `hash`
-- (HashJoin) and `direct` (DirectKeyValueJoin), which unconditionally stream the left side through
-- once. Every other algorithm (partial_merge / prefer_partial_merge / full_sorting_merge / auto /
-- parallel_hash / grace_hash) is treated as possibly reordering.
--
-- The physical joins for the merge algorithms return `preservesLeftBlockOrder() == false`, so the
-- read-in-order-through-join second pass (findReadingStep in optimizeReadInOrder.cpp) refuses to
-- propagate the left sort order through the join. Before the fix, topKThroughJoin's pass-1 deferral
-- only checked for delayed blocks, so it still deferred to that second pass; the second pass then
-- bailed and NEITHER optimization fired, leaving a full join plus a full sort. The deferral now also
-- requires order preservation, so the pushdown runs in pass 1 for these algorithms.
--
-- All plan-affecting settings are pinned in each query's SETTINGS clause (query-level settings
-- take precedence over the CI settings randomizer): read-in-order enabled so the deferral path
-- is live; spilling off so joinMayHaveDelayedBlocks is false; join side pinned so the deferral
-- is otherwise satisfiable and order preservation is the only remaining gate. Parallel
-- replicas is pinned off too: it is a plan-affecting setting the ParallelReplicas CI variant
-- turns on in the default profile, under which the MergeTree read is remote and the local
-- read-in-order / topK plan (and the direct key-value join path) do not apply.
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

-- A second key shape (UInt8 join key). The deferral no longer inspects the key type, so this behaves
-- like the UInt64 shape; kept to confirm the parallel_hash pushdown is independent of the key type.
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
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
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
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
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
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%';

-- parallel_hash is not one of the algorithms the logical deferral proves order-preserving: only
-- `hash` / `direct` (which unconditionally stream the left side through once) are. The physical
-- ConcurrentHashJoin preserves the left order only for some slot-count / map-type combinations that
-- are not known at this logical stage, so the deferral treats parallel_hash conservatively as
-- possibly reordering and fires the pushdown here (count = 1). The second probe is an invariant: the
-- pushed inner Sort is still satisfied by a plain read-in-order scan directly above the MergeTree
-- read (count > 0). Both hold regardless of the key shape (UInt64 here, UInt8/key8 below, multi-key,
-- or a residual filter) and the slot count (multi-slot here, single-slot below) - the deferral no
-- longer inspects either.
--
-- enable_analyzer is pinned to 1: this deferral runs on the logical JoinStepLogical, which only
-- exists in the analyzer; under the old analyzer topKThroughJoin sees the already-built physical
-- ConcurrentHashJoin and reads its exact preservesLeftBlockOrder(), a different (also correct) path.
-- The stateless runner randomizes max_threads and the in-order trio, so both are pinned per query.
-- Pinned on both the wrapper and the EXPLAIN because the old-analyzer job forbids changing
-- enable_analyzer in a subquery.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
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
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain ILIKE '%Read type: InOrder%'
SETTINGS enable_analyzer = 1;

-- Same parallel_hash contract with a UInt8 key: the pushdown fires (count > 0) independent of the
-- key type, since the deferral no longer inspects it.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl8_04499.pk, tr8_04499.v
    FROM tl8_04499 LEFT ALL JOIN tr8_04499 ON tl8_04499.j = tr8_04499.j
    ORDER BY tl8_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

-- A `direct,parallel_hash,hash` list: the deferral proves order preservation only if EVERY configured
-- algorithm is order-preserving. `parallel_hash` is in the list and is not proven order-preserving, so
-- the whole list is treated as possibly reordering and the pushdown fires.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl8_04499.pk, tr8_04499.v
    FROM tl8_04499 LEFT ALL JOIN tr8_04499 ON tl8_04499.j = tr8_04499.j
    ORDER BY tl8_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'direct,parallel_hash,hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

-- Single-slot parallel_hash (max_threads = 1): the physical ConcurrentHashJoin would preserve the
-- left order here, but the logical deferral does not special-case the slot count (it no longer reads
-- max_threads), so it still treats parallel_hash conservatively and fires the pushdown (count = 1).
-- This is a perf-only difference (an explicit Sort + Limit instead of read-in-order-through-join),
-- never a correctness one. Second probe is the invariant: the pushed inner Sort still reads InOrder
-- from the MergeTree primary key (count > 0). max_threads = 1 and the in-order trio are pinned
-- per-query (the stateless runner randomizes both).
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 1,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
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
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
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
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%';

-- More complex ON shapes (a multi-key equi-join, and a single equi-key plus a residual range filter)
-- behave the same as the plain single-key shape: parallel_hash is treated conservatively regardless
-- of the key shape, so the pushdown fires (count = 1). The deferral no longer inspects the ON
-- expression. The second probe of each pair is the invariant: the pushed inner Sort reads InOrder
-- from the MergeTree primary key (count > 0).
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j AND tl_04499.pk = tr_04499.v
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j AND tl_04499.pk = tr_04499.v
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain ILIKE '%Read type: InOrder%'
SETTINGS enable_analyzer = 1;

-- Single equi-key + residual range filter (`tl.pk > tr.v`): same as above, the pushdown fires
-- (count = 1) since parallel_hash is treated conservatively regardless of the residual filter.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j AND tl_04499.pk > tr_04499.v
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, tr_04499.v
    FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j AND tl_04499.pk > tr_04499.v
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'parallel_hash', enable_analyzer = 1, max_threads = 8,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
) WHERE explain ILIKE '%Read type: InOrder%'
SETTINGS enable_analyzer = 1;

-- Correctness pins the pushed limit's value and side. Because the join is one-to-one, the top-10
-- pk are exactly 0..9: a pushdown that keeps only the first row (a wrong `Limit 1`) would return a
-- single 0, and a limit pushed onto the right side would drop or reorder left rows.
SELECT tl_04499.pk
FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
ORDER BY tl_04499.pk LIMIT 10
SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SELECT tl_04499.pk
FROM tl_04499 LEFT ALL JOIN tr_04499 ON tl_04499.j = tr_04499.j
ORDER BY tl_04499.pk LIMIT 10
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

-- The pass-1 deferral must be algorithm-aware about spilling. A nonzero max_bytes_*_before_external_join
-- (the default max_bytes_ratio_before_external_join = 0.5) only wraps hash-family algorithms
-- (hash / parallel_hash / prefer_partial_merge / default) in SpillingHashJoin. join_algorithm = 'direct'
-- builds a DirectKeyValueJoin, which never spills and preserves the left order, so it must still defer to
-- the read-in-order-through-join second pass. Before joinMayHaveDelayedBlocks keyed on the enabled
-- algorithm, any nonzero spill setting flagged direct as may-delay and forced the explicit Sort + Limit
-- pushdown even though the physical join cannot produce delayed blocks.
--
-- Spilling is deliberately NOT pinned off here (the default 0.5 ratio is the case under test). The right
-- side is a Join engine so the direct key-value path applies. First probe: NO explicit pushed Limit on the
-- left (count = 0), i.e. the deferral fired. Second probe: the left MergeTree read streams InOrder (> 0).
DROP TABLE IF EXISTS trd_04499;
CREATE TABLE trd_04499 (j UInt64, v UInt64) ENGINE = Join(ANY, LEFT, j)
    AS SELECT number, number FROM numbers(100000);

SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, trd_04499.v
    FROM tl_04499 LEFT ANY JOIN trd_04499 ON tl_04499.j = trd_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'direct', enable_analyzer = 1,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0
) WHERE explain LIKE '%Limit' AND explain NOT LIKE '%LIMIT%'
SETTINGS enable_analyzer = 1;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT tl_04499.pk, trd_04499.v
    FROM tl_04499 LEFT ANY JOIN trd_04499 ON tl_04499.j = trd_04499.j
    ORDER BY tl_04499.pk LIMIT 10
    SETTINGS join_algorithm = 'direct', enable_analyzer = 1,
        query_plan_enable_optimizations = 1, optimize_read_in_order = 1,
        query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
        query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000,
        query_plan_join_swap_table = 0, enable_parallel_replicas = 0
) WHERE explain ILIKE '%Read type: InOrder%'
SETTINGS enable_analyzer = 1;

-- Correctness of the recovered read-in-order-through-join plan: the top-10 pk are exactly 0..9.
SELECT tl_04499.pk
FROM tl_04499 LEFT ANY JOIN trd_04499 ON tl_04499.j = trd_04499.j
ORDER BY tl_04499.pk LIMIT 10
SETTINGS join_algorithm = 'direct', enable_analyzer = 1, query_plan_join_swap_table = 0, enable_parallel_replicas = 0;

DROP TABLE tl_04499;
DROP TABLE tr_04499;
DROP TABLE tl8_04499;
DROP TABLE tr8_04499;
DROP TABLE trd_04499;

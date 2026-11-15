WITH
    (SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC') AS jemalloc_enabled,
    (SELECT count() FROM system.jemalloc_arena_bins) AS total_rows,
    (SELECT uniqExact(arena) FROM system.jemalloc_arena_bins) AS arenas
SELECT
    (total_rows > 0) = jemalloc_enabled,
    (arenas > 0) = jemalloc_enabled;

-- Bin geometry is a per-class constant, identical in every arena and in the aggregated table.
SELECT countIf(NOT consistent) = 0
FROM
(
    SELECT a.size = b.size AND a.nregs = b.nregs AND a.slab_size = b.slab_size AS consistent
    FROM system.jemalloc_arena_bins AS a
    INNER JOIN system.jemalloc_bins AS b ON a.index = b.index AND a.large = b.large
);

-- Large size classes are not slab-based; their slab columns are always zero.
-- (No assertions relating two live counters: they are separate mallctl reads
-- and can come from different stats epochs.)
SELECT countIf(slab_size != 0 OR nonfull_slabs != 0 OR waste != 0) = 0 FROM system.jemalloc_arena_bins WHERE large;

-- Dedicated long-lived arenas are labeled, and the label is constant within an arena.
SELECT
    (SELECT countIf(purpose NOT IN ('', 'mergetree', 'jit', 'cache')) FROM system.jemalloc_arena_bins) = 0,
    (SELECT countIf(purposes != 1) FROM (SELECT arena, uniqExact(purpose) AS purposes FROM system.jemalloc_arena_bins GROUP BY arena)) = 0;

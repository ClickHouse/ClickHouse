-- Verify that the dedicated JIT jemalloc arena and the compiled-expression cache configuration
-- limits are exposed in `system.asynchronous_metrics`. Both sets of metrics are gated on build
-- options, so the test compares "metric is exposed" against the build options it depends on.

-- Dedicated JIT arena metrics exist only when jemalloc and the embedded compiler are built in
-- and the platform supports `thread.arena` routing for `CHJIT`.
WITH
    (SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_JEMALLOC') AS jemalloc_on,
    (SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_EMBEDDED_COMPILER') AS jit_on,
    (SELECT value = 'Darwin' FROM system.build_options WHERE name = 'SYSTEM') AS is_darwin
SELECT
    (count() > 0) = (jemalloc_on AND jit_on AND NOT is_darwin)
FROM system.asynchronous_metrics
WHERE metric LIKE 'jemalloc.jit_arena%';

-- The compiled-expression cache configuration limits exist whenever the embedded compiler is built in.
WITH
    (SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_EMBEDDED_COMPILER') AS jit_on
SELECT count() = if(jit_on, 2, 0)
FROM system.asynchronous_metrics
WHERE metric IN ('CompiledExpressionCacheBytesMax', 'CompiledExpressionCacheCountMax');

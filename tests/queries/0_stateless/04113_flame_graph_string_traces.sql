-- Tests that flameGraph can accept Array(String) traces in addition to Array(UInt64).
-- The Array(String) form is useful for data where addresses have already been symbolized
-- (e.g. system.trace_log + arrayMap(addressToSymbol, trace), or any external profiler dump).

SET allow_introspection_functions = 1;

-- Single trace with the default size (1).
SELECT 'single trace';
SELECT arrayJoin(flameGraph(['main', 'a', 'b'])) AS line ORDER BY line;

-- Multiple aggregated traces. Common prefix `main` collapses; only leaves (self > 0) are emitted.
SELECT 'aggregated traces';
SELECT arrayJoin(flameGraph(t)) AS line
FROM values('t Array(String)', (['main', 'a']), (['main', 'a']), (['main', 'b']))
ORDER BY line;

-- Memory profiling via (size, ptr): the dealloc cancels the matching alloc by ptr/size.
SELECT 'alloc/dealloc by ptr';
SELECT arrayJoin(flameGraph(t, s, p)) AS line
FROM values('t Array(String), s Int64, p UInt64',
    (['main', 'a'], 100, 1),
    (['main', 'b'], 30, 2),
    (['main', 'a'], -100, 1))
ORDER BY line;

-- Pointer mode still works after the change. The output uses `0x...` hex for unresolved addresses,
-- which is platform-dependent only in width — the trace shape is one line for one trace.
SELECT 'pointer mode';
SELECT length(flameGraph([42::UInt64, 100::UInt64])) AS num_lines;

-- Reject types that are neither Array(UInt64) nor Array(String).
SELECT flameGraph([1.5, 2.5]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT flameGraph([1::UInt32, 2::UInt32]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

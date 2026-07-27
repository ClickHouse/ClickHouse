-- The UntrackedMemory async metric sums per-thread untracked_memory counters
-- via UntrackedMemoryRegistry. It must be exposed by AsynchronousMetrics.

SELECT count() FROM system.asynchronous_metrics WHERE metric = 'UntrackedMemory';

-- Value is an Int64 sum; can be negative if threads have net-freed since last flush,
-- but the metric must exist and be queryable as a number.
SELECT isFinite(value) FROM system.asynchronous_metrics WHERE metric = 'UntrackedMemory';

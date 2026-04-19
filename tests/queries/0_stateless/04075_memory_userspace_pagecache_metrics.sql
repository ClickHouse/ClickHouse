-- Verify CGroupMemoryUsedWithoutPageCache is registered and <= CGroupMemoryUsed.
-- Snapshot both metrics in a single read to avoid races between asynchronous_metrics updates.
-- On environments without cgroups, both metrics are absent, so we skip value checks.
WITH
    (SELECT groupArray((metric, value)) FROM system.asynchronous_metrics
     WHERE metric IN ('CGroupMemoryUsed', 'CGroupMemoryUsedWithoutPageCache')) AS metrics,
    arrayCount(x -> x.1 = 'CGroupMemoryUsed', metrics) AS has_used,
    arrayCount(x -> x.1 = 'CGroupMemoryUsedWithoutPageCache', metrics) AS has_without_cache,
    arrayFirst(x -> x.1 = 'CGroupMemoryUsed', metrics) AS used,
    arrayFirst(x -> x.1 = 'CGroupMemoryUsedWithoutPageCache', metrics) AS without_cache
SELECT
    -- Both metrics must be either both present or both absent.
    has_used = has_without_cache,
    -- When present, without_cache must be <= used.
    if(has_used = 1, without_cache.2 <= used.2, 1);

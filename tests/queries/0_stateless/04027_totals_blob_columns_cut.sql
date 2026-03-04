-- Tags: distributed

-- Regression test for ColumnBLOB assertion failure in prepareTotals.
-- When a distributed query with WITH TOTALS and arrayJoin in WHERE/QUALIFY
-- is executed across multiple shards, the totals may have multiple rows
-- (expanded by arrayJoin). BlocksMarshallingStep wraps columns in ColumnBLOB
-- for network transfer. prepareTotals then tries to cut the totals to 1 row,
-- but ColumnBLOB::cloneEmpty returns the wrapped column type, causing
-- an assertion failure in debug/sanitizer builds.

SELECT DISTINCT x
FROM remote('127.0.0.{1,2}', view(
    SELECT 1 AS x GROUP BY 'a' WITH TOTALS
))
WHERE arrayJoin([1, 1]) = x
SETTINGS parallel_replicas_for_cluster_engines = 0;

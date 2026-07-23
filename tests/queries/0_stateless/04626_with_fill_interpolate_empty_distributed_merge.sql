-- Tags: shard

-- Issue #111547: WITH FILL ... INTERPOLATE over a network merge of two empty sorted streams
-- used to abort in FillingTransform::saveLastRow (PODArray out-of-bounds). Must return the
-- filled suffix. The WITH FILL is kept in a subquery with DISTINCT applied OUTSIDE it, so the
-- Filling step still receives the duplicated interpolate column that triggers the bug (an inner
-- DISTINCT would collapse it before Filling and stop exercising the fix), while the outer DISTINCT
-- keeps the reference independent of the separate per-shard WITH FILL duplication (#111212 /
-- #111439). Runs on both analyzers.
SELECT DISTINCT n, inter FROM (
    SELECT n, inter
    FROM remote('127.0.0.1,127.0.0.2', view(
        SELECT number AS inter, toFloat32(number / 10) AS n FROM numbers(10) WHERE 0))
    ORDER BY n ASC NULLS LAST WITH FILL FROM 0 TO 11.51 STEP 2. INTERPOLATE (`inter` AS 1023)
    SETTINGS prefer_localhost_replica = 0)
ORDER BY n;

-- Duplicate INTERPOLATE targets are rejected on both analyzers (the old analyzer already
-- rejected them; the planner path used to build duplicate destination positions and abort).
SELECT n, inter FROM remote('127.0.0.1,127.0.0.2', view(
    SELECT number AS inter, toFloat32(number / 10) AS n FROM numbers(10) WHERE 0))
ORDER BY n WITH FILL FROM 0 TO 6 STEP 2 INTERPOLATE (`inter` AS 1, `inter` AS 2)
SETTINGS prefer_localhost_replica = 0; -- { serverError INVALID_WITH_FILL_EXPRESSION }

-- Empty INTERPOLATE () when two output columns resolve to the same block name (`a AS a, a AS a2`).
-- InterpolateDescription used to list that destination twice, so FillingTransform appended to one
-- output column twice per generated row and produced a ragged block. Must fill correctly on both
-- analyzers (the old analyzer's empty-INTERPOLATE path was the reachable one).
SELECT '--- empty INTERPOLATE, repeated destination name ---';
SELECT n, a AS a, a AS a2 FROM (SELECT toFloat32(number) AS n, number * 10 AS a FROM numbers(2))
ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE ()
SETTINGS enable_analyzer = 0;
SELECT n, a AS a, a AS a2 FROM (SELECT toFloat32(number) AS n, number * 10 AS a FROM numbers(2))
ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE ()
SETTINGS enable_analyzer = 1;

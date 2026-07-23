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

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS test_04228 SYNC;
CREATE TABLE test_04228 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_04228 SELECT number FROM numbers(1000);

-- Issue #102320. The CTE `a` is referenced once at top level and once deeply
-- nested inside a WHERE id IN (... WHERE id IN (...)) on a MergeTree-PK column.
-- Before the fix, PK pruning on test_04228 triggered buildOrderedSetInplace for
-- the inner IN-subquery's set during the recursive plan->optimize launched by
-- main's addStepsToBuildSets, but main's resolveMaterializingCTEs had already
-- claimed a.plan at the outer level -- so the safety-net
-- DelayedMaterializingCTEsStep inside the inner-IN plan degraded to a
-- pass-through and the synchronously executed pipeline read from an empty
-- a.storage, throwing LOGICAL_ERROR.
SELECT count() FROM (
    WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(100))
    SELECT id FROM a
    WHERE id IN (
        SELECT id FROM test_04228
        WHERE id IN (SELECT id FROM a GROUP BY id)
        GROUP BY id
    )
    GROUP BY id
);

DROP TABLE test_04228;

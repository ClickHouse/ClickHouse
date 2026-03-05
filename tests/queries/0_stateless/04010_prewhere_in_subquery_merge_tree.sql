-- Regression test: PREWHERE with IN subquery on MergeTree should not throw
-- "Not-ready Set is passed as the second argument for function 'in'"
-- The issue was that sets in PREWHERE were not built synchronously during applyFilters(),
-- and the pipeline-level CreatingSetsStep might not complete before PREWHERE evaluation
-- due to a race condition (e.g. when JoiningTransform closes the pipeline early).
-- The fix builds PREWHERE sets synchronously in ReadFromMergeTree::applyFilters.
-- See also: #97235, #89557

DROP TABLE IF EXISTS test_local;
DROP TABLE IF EXISTS test_merge;

CREATE TABLE test_local (key String, value String)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO test_local VALUES ('a', 'x'), ('b', 'y');

CREATE TABLE test_merge AS test_local
ENGINE = Merge(currentDatabase(), 'test_local');

-- Case 1: PREWHERE on a key column (set built during index analysis via buildOrderedSetInplace)
SELECT count() FROM test_local PREWHERE key IN (SELECT 'a');
SELECT count() FROM test_merge PREWHERE key IN (SELECT 'a');

-- Case 2: PREWHERE on a non-key column (set is NOT built during index analysis;
--         relies on the fix in applyFilters to build it synchronously)
SELECT count() FROM test_local PREWHERE value IN (SELECT 'x');
SELECT count() FROM test_merge PREWHERE value IN (SELECT 'x');

-- Case 3: Same as case 2 but with use_index_for_in_with_subqueries=0
-- (prevents buildOrderedSetInplace from building the set, making the fix essential)
SELECT count() FROM test_local PREWHERE value IN (SELECT 'x') SETTINGS use_index_for_in_with_subqueries=0;
SELECT count() FROM test_merge PREWHERE value IN (SELECT 'x') SETTINGS use_index_for_in_with_subqueries=0;

-- Case 4: Self-referencing subquery
SELECT count() FROM test_local PREWHERE value IN (SELECT value FROM test_local);
SELECT count() FROM test_merge PREWHERE value IN (SELECT value FROM test_merge);

DROP TABLE test_merge;
DROP TABLE test_local;

-- Regression test: PREWHERE with IN subquery on MergeTree should not throw
-- "Not-ready Set is passed as the second argument for function 'in'"
-- The issue was that sets in PREWHERE were not built synchronously during applyFilters(),
-- and the pipeline-level CreatingSetsStep might not complete before PREWHERE evaluation.

DROP TABLE IF EXISTS test_local;
DROP TABLE IF EXISTS test_merge;

CREATE TABLE test_local (name String)
ENGINE = MergeTree
ORDER BY name AS SELECT 'x';

CREATE TABLE test_merge AS test_local
ENGINE = Merge(currentDatabase(), 'test_local');

SELECT count() FROM test_merge PREWHERE name IN (SELECT name FROM test_local);
SELECT count() FROM test_merge PREWHERE name IN (SELECT name FROM test_merge);
SELECT count() FROM test_local PREWHERE name IN (SELECT name FROM test_local);

DROP TABLE test_merge;
DROP TABLE test_local;

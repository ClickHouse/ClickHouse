-- Tests that StorageMerge::getModificationHash fails closed (returns NULL) on a cycle of mutually-referencing
-- Merge tables instead of recursing until the stack is exhausted (issue #108713).
--
-- Two Merge tables m1 and m2 both match the regexp 'm.*', so each includes the other as a source.
-- traverseTablesUntil skips only `this`, so evaluating system.tables.modification_hash for m1 would recurse
-- m1 -> m2 -> m1 -> ... A thread-local re-entry guard (matching StorageDistributed::getModificationHash)
-- detects the repeat and returns NULL, so the whole Merge table fails closed promptly.

DROP TABLE IF EXISTS m1;
DROP TABLE IF EXISTS m2;
DROP TABLE IF EXISTS m_base;

-- A cycle of two Merge tables that reference each other (both match 'm.*'): fail closed, return NULL. With the
-- re-entry guard this returns promptly; without it the query would recurse until checkStackSize throws.
CREATE TABLE m1 (x UInt64) ENGINE = Merge(currentDatabase(), 'm.*');
CREATE TABLE m2 (x UInt64) ENGINE = Merge(currentDatabase(), 'm.*');

SELECT 'merge cycle null (m1)', modification_hash IS NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'm1';
SELECT 'merge cycle null (m2)', modification_hash IS NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'm2';

DROP TABLE m1;
DROP TABLE m2;

-- Positive control: a Merge table over a plain MergeTree source (no cycle) reports a non-NULL hash, so the
-- re-entry guard does not over-fire on the ordinary path.
CREATE TABLE m_base (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO m_base VALUES (1);
CREATE TABLE m1 (x UInt64) ENGINE = Merge(currentDatabase(), '^m_base$');

SELECT 'merge non-cycle not null', modification_hash IS NOT NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'm1';

DROP TABLE m1;
DROP TABLE m_base;

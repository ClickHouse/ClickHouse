-- Regression test for issue #104740 / STID 3593-560a / 3593-5990.
--
-- When the regexp of a `Merge` table matches an `Alias` storage whose target
-- has been dropped, the query previously aborted with an internal
-- `LOGICAL_ERROR` "Table has no columns.". This is reachable in stress tests
-- via `ignore_drop_queries_probability` (a dangling alias leaks across tests),
-- and in regular workloads via a TOCTOU race when another session drops the
-- target between `StorageMerge::getSelectedTables` and the metadata snapshot
-- read in `ReadFromMerge::createChildrenPlans`.
--
-- The fix turns the assertion into a user-facing `UNKNOWN_TABLE` exception
-- that names the dangling alias.

SET allow_experimental_alias_table_engine = 1;

DROP TABLE IF EXISTS source_target_104740;
DROP TABLE IF EXISTS alias_with_missing_target_104740;
DROP TABLE IF EXISTS m_merge_104740;
DROP TABLE IF EXISTS b_join_104740;

CREATE TABLE source_target_104740 (key UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO source_target_104740 VALUES (1);

CREATE TABLE alias_with_missing_target_104740 ENGINE = Alias('source_target_104740');

-- Sanity: the alias reads through the target before we drop it.
SELECT * FROM alias_with_missing_target_104740;

-- Drop the target; the alias is now dangling.
DROP TABLE source_target_104740;

-- A `Merge` whose regexp matches the dangling alias must throw a clear
-- `UNKNOWN_TABLE` exception, not `LOGICAL_ERROR` "Table has no columns.".
CREATE TABLE m_merge_104740 (key UInt32) ENGINE = Merge(currentDatabase(), 'alias_with_missing_target_104740');
CREATE TABLE b_join_104740 (key UInt32, ID UInt32) ENGINE = MergeTree ORDER BY key;

SELECT * FROM m_merge_104740; -- { serverError UNKNOWN_TABLE }
SELECT * FROM m_merge_104740 INNER JOIN b_join_104740 USING (key) WHERE ID = 1; -- { serverError UNKNOWN_TABLE }

DROP TABLE alias_with_missing_target_104740;
DROP TABLE m_merge_104740;
DROP TABLE b_join_104740;

-- Tags: zookeeper

-- Settings allow_deprecated_syntax_for_merge_tree prevent to enable the is_deleted column
set allow_deprecated_syntax_for_merge_tree=0;

-- Test the bahaviour without the is_deleted column
DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version) Order by (uid) settings allow_experimental_replacing_merge_with_cleanup=1;
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
SELECT '== Test SELECT ... FINAL - no is_deleted ==';
select * from test FINAL order by uid;
OPTIMIZE TABLE test FINAL CLEANUP;
select * from test order by uid;

DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version) Order by (uid) SETTINGS clean_deleted_rows='Always', allow_experimental_replacing_merge_with_cleanup=1;
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
SELECT '== Test SELECT ... FINAL - no is_deleted SETTINGS clean_deleted_rows=Always ==';
select * from test FINAL order by uid;
OPTIMIZE TABLE test FINAL CLEANUP;
select * from test order by uid;

-- Test the new behaviour
DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid) settings allow_experimental_replacing_merge_with_cleanup=1;
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
SELECT '== Test SELECT ... FINAL ==';
select * from test FINAL order by uid;
select * from test order by uid;

SELECT '== Insert backups ==';
INSERT INTO test (*) VALUES ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1);
select * from test FINAL order by uid;

SELECT '== Insert a second batch with overlaping data ==';
INSERT INTO test (*) VALUES ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 1), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0), ('d2', 2, 1), ('d2', 3, 0), ('d3', 2, 1), ('d3', 3, 0);
select * from test FINAL order by uid;

DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid) settings allow_experimental_replacing_merge_with_cleanup=1;

-- Expect d6 to be version=3 is_deleted=false
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d1', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d1', 5, 0), ('d2', 1, 0), ('d3', 1, 0), ('d4', 1, 0),  ('d5', 1, 0), ('d6', 1, 0), ('d6', 3, 0);
-- Insert previous version of 'd6' but only v=3 is_deleted=false will remain
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d1', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d1', 5, 0), ('d2', 1, 0), ('d3', 1, 0), ('d4', 1, 0),  ('d5', 1, 0), ('d6', 1, 0), ('d6', 2, 1);
SELECT '== Only last version remains after OPTIMIZE W/ CLEANUP ==';
OPTIMIZE TABLE test FINAL CLEANUP;
select * from test order by uid;

-- insert d6 v=3 is_deleted=true (timestamp more recent so this version should be the one take into acount)
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d1', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d1', 5, 0), ('d2', 1, 0), ('d3', 1, 0), ('d4', 1, 0),  ('d5', 1, 0), ('d6', 1, 0), ('d6', 3, 1);

SELECT '== OPTIMIZE W/ CLEANUP (remove d6) ==';
OPTIMIZE TABLE test FINAL CLEANUP;
-- No d6 anymore
select * from test order by uid;

DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid) SETTINGS clean_deleted_rows='Always', allow_experimental_replacing_merge_with_cleanup=1;

SELECT '== Test of the SETTINGS clean_deleted_rows as Always ==';
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
-- Even if the setting is set to Always, the SELECT FINAL doesn't delete rows
select * from test FINAL order by uid;
select * from test order by uid;

OPTIMIZE TABLE test FINAL;
-- d6 has to be removed since we set clean_deleted_rows as 'Always'
select * from test where is_deleted=0 order by uid;

SELECT '== Test of the SETTINGS clean_deleted_rows as Never ==';
ALTER TABLE test MODIFY SETTING clean_deleted_rows='Never';
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
OPTIMIZE TABLE test FINAL;
-- d6 has NOT to be removed since we set clean_deleted_rows as 'Never'
select * from test order by uid;

DROP TABLE IF EXISTS testCleanupR1;

CREATE TABLE testCleanupR1 (uid String, version UInt32, is_deleted UInt8)
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{database}/tables/test_cleanup/', 'r1', version, is_deleted)
    ORDER BY uid settings allow_experimental_replacing_merge_with_cleanup=1;


INSERT INTO testCleanupR1 (*) VALUES ('d1', 1, 0),('d2', 1, 0),('d3', 1, 0),('d4', 1, 0);
INSERT INTO testCleanupR1 (*) VALUES ('d3', 2, 1);
INSERT INTO testCleanupR1 (*) VALUES ('d1', 2, 1);
SYSTEM SYNC REPLICA testCleanupR1; -- Avoid "Cannot select parts for optimization: Entry for part all_2_2_0 hasn't been read from the replication log yet"

OPTIMIZE TABLE testCleanupR1 FINAL CLEANUP;

-- Only d3 to d5 remain
SELECT '== (Replicas) Test optimize ==';
SELECT * FROM testCleanupR1 order by uid;

------------------------------

DROP TABLE IF EXISTS testSettingsR1;

CREATE TABLE testSettingsR1 (col1 String, version UInt32, is_deleted UInt8)
    ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{database}/tables/test_setting/', 'r1', version, is_deleted)
    ORDER BY col1
    SETTINGS clean_deleted_rows = 'Always', allow_experimental_replacing_merge_with_cleanup=1;

INSERT INTO testSettingsR1 (*) VALUES ('c1', 1, 1),('c2', 1, 0),('c3', 1, 1),('c4', 1, 0);
SYSTEM SYNC REPLICA testSettingsR1; -- Avoid "Cannot select parts for optimization: Entry for part all_2_2_0 hasn't been read from the replication log yet"

OPTIMIZE TABLE testSettingsR1 FINAL;

-- Only d3 to d5 remain
SELECT '== (Replicas) Test settings ==';
SELECT * FROM testSettingsR1 where is_deleted=0 order by col1;


------------------------------
-- Check errors
DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid) settings allow_experimental_replacing_merge_with_cleanup=1;

-- is_deleted == 0/1
INSERT INTO test (*) VALUES ('d1', 1, 2); -- { serverError INCORRECT_DATA }

DROP TABLE IF EXISTS test;
-- checkis_deleted type
CREATE TABLE test (uid String, version UInt32, is_deleted String) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid); -- { serverError BAD_TYPE_OF_FIELD }

CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid);
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
select 'no cleanup 1', * from test FINAL order by uid;
OPTIMIZE TABLE test FINAL CLEANUP; -- { serverError SUPPORT_IS_DISABLED }
select 'no cleanup 2', * from test order by uid;
DROP TABLE test;

CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{database}/tables/no_cleanup/', 'r1', version, is_deleted) Order by (uid);
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
select 'no cleanup 3', * from test FINAL order by uid;
OPTIMIZE TABLE test FINAL CLEANUP; -- { serverError SUPPORT_IS_DISABLED }
select 'no cleanup 4', * from test order by uid;
DROP TABLE test;

-- is_deleted column for other mergeTrees - ErrorCodes::LOGICAL_ERROR)

-- Check clean_deleted_rows='Always' for other MergeTrees
SELECT '== Check cleanup & settings for other merge trees ==';
CREATE TABLE testMT (uid String, version UInt32, is_deleted UInt8) ENGINE = MergeTree() Order by (uid) SETTINGS clean_deleted_rows='Always', allow_experimental_replacing_merge_with_cleanup=1;
INSERT INTO testMT (*) VALUES ('d1', 1, 1);
OPTIMIZE TABLE testMT FINAL CLEANUP;  -- { serverError CANNOT_ASSIGN_OPTIMIZE }
OPTIMIZE TABLE testMT FINAL;
SELECT * FROM testMT order by uid;

CREATE TABLE testSummingMT (uid String, version UInt32, is_deleted UInt8) ENGINE = SummingMergeTree() Order by (uid) SETTINGS clean_deleted_rows='Always', allow_experimental_replacing_merge_with_cleanup=1;
INSERT INTO testSummingMT (*) VALUES ('d1', 1, 1);
OPTIMIZE TABLE testSummingMT FINAL CLEANUP;  -- { serverError CANNOT_ASSIGN_OPTIMIZE }
OPTIMIZE TABLE testSummingMT FINAL;
SELECT * FROM testSummingMT order by uid;

CREATE TABLE testAggregatingMT (uid String, version UInt32, is_deleted UInt8) ENGINE = AggregatingMergeTree() Order by (uid) SETTINGS clean_deleted_rows='Always', allow_experimental_replacing_merge_with_cleanup=1;
INSERT INTO testAggregatingMT (*) VALUES ('d1', 1, 1);
OPTIMIZE TABLE testAggregatingMT FINAL CLEANUP;  -- { serverError CANNOT_ASSIGN_OPTIMIZE }
OPTIMIZE TABLE testAggregatingMT FINAL;
SELECT * FROM testAggregatingMT order by uid;

CREATE TABLE testCollapsingMT (uid String, version UInt32, is_deleted UInt8, sign Int8) ENGINE = CollapsingMergeTree(sign) Order by (uid) SETTINGS clean_deleted_rows='Always', allow_experimental_replacing_merge_with_cleanup=1;
INSERT INTO testCollapsingMT (*) VALUES ('d1', 1, 1, 1);
OPTIMIZE TABLE testCollapsingMT FINAL CLEANUP;  -- { serverError CANNOT_ASSIGN_OPTIMIZE }
OPTIMIZE TABLE testCollapsingMT FINAL;
SELECT * FROM testCollapsingMT order by uid;

CREATE TABLE testVersionedCMT (uid String, version UInt32, is_deleted UInt8, sign Int8) ENGINE = VersionedCollapsingMergeTree(sign, version) Order by (uid) SETTINGS clean_deleted_rows='Always', allow_experimental_replacing_merge_with_cleanup=1;
INSERT INTO testVersionedCMT (*) VALUES ('d1', 1, 1, 1);
OPTIMIZE TABLE testVersionedCMT FINAL CLEANUP;  -- { serverError CANNOT_ASSIGN_OPTIMIZE }
OPTIMIZE TABLE testVersionedCMT FINAL;
SELECT * FROM testVersionedCMT order by uid;

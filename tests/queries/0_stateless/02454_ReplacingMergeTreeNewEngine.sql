DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid);

INSERT INTO test (*) VALUES ('d1', 1, 0), ('d2', 1, 0), ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0);
select * from test FINAL;

-- Test inserting Backup
INSERT INTO test (*) VALUES ('d6', 1, 0), ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1);
select * from test FINAL;

-- test insert second batch with overlaping data
INSERT INTO test (*) VALUES ('d4', 1, 0), ('d6', 2, 1), ('d3', 1, 0), ('d1', 2, 1), ('d5', 1, 0), ('d4', 2, 1), ('d1', 3, 1), ('d1', 4, 1), ('d4', 3, 0), ('d1', 5, 0), ('d2', 2, 1), ('d2', 3, 0), ('d3', 2, 1), ('d3', 3, 0);
select * from test FINAL;

DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid);

INSERT INTO test (*) VALUES ('d1', 1, 0), ('d1', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d1', 5, 0), ('d2', 1, 0), ('d3', 1, 0), ('d4', 1, 0),  ('d5', 1, 0), ('d6', 1, 0), ('d6', 2, 1);
OPTIMIZE TABLE test FINAL CLEANUP;
-- Expect d6 to be removed
select * from test;

-- Expect d6 to be version=3 is_deleted=false
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d1', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d1', 5, 0), ('d2', 1, 0), ('d3', 1, 0), ('d4', 1, 0),  ('d5', 1, 0), ('d6', 1, 0), ('d6', 3, 0);
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d1', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d1', 5, 0), ('d2', 1, 0), ('d3', 1, 0), ('d4', 1, 0),  ('d5', 1, 0), ('d6', 1, 0), ('d6', 2, 1);
-- Both versions exist
select * from test;

OPTIMIZE TABLE test FINAL CLEANUP;
-- Only v=3 id=false remains
select * from test;

-- insert d6 v=3 is_deleted=true (timestamp more recent so this version should be the one take into acount)
INSERT INTO test (*) VALUES ('d1', 1, 0), ('d1', 2, 1), ('d1', 3, 0), ('d1', 4, 1), ('d1', 5, 0), ('d2', 1, 0), ('d3', 1, 0), ('d4', 1, 0),  ('d5', 1, 0), ('d6', 1, 0), ('d6', 3, 1);
OPTIMIZE TABLE test FINAL CLEANUP;
-- No d6
select * from test;

DROP TABLE IF EXISTS test;

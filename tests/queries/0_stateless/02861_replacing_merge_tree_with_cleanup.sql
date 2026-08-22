DROP TABLE IF EXISTS test;
CREATE TABLE test (uid String, version UInt32, is_deleted UInt8) ENGINE = ReplacingMergeTree(version, is_deleted) Order by (uid) SETTINGS vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 0,
    min_rows_for_wide_part = 1,
    min_bytes_for_wide_part = 1,
    allow_experimental_replacing_merge_with_cleanup=1;

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

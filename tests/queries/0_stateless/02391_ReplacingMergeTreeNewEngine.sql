CREATE TABLE test (uid String, sign Int8, version UInt32) ENGINE = ReplacingMergeTree(sign, version) Order by (uid);

INSERT INTO test (*) VALUES ('d1', 1, 1), ('d2', 1, 1), ('d6', 1, 1), ('d4', 1, 1), ('d6', -1, 2), ('d3', 1, 1), ('d1', -1, 2), ('d5', 1, 1), ('d4', -1, 2), ('d1', 1, 3), ('d1', -1, 4), ('d4', 1, 3), ('d1', 1, 5);
select * from test FINAL;

-- Test inserting Backup
INSERT INTO test (*) VALUES ('d6', 1, 1), ('d4', 1, 1), ('d6', -1, 2), ('d3', 1, 1), ('d1', -1, 2), ('d5', 1, 1), ('d4', -1, 2);
select * from test FINAL;

-- test insert second batch with overlaping data
INSERT INTO test (*) VALUES ('d4', 1, 1), ('d6', -1, 2), ('d3', 1, 1), ('d1', -1, 2), ('d5', 1, 1), ('d4', -1, 2), ('d1', 1, 3), ('d1', -1, 4), ('d4', 1, 3), ('d1', 1, 5), ('d2', -1, 2), ('d2', 1, 3), ('d3', -1, 2), ('d3', 1, 3);
select * from test FINAL;

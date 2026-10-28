-- Tags: shard

-- A Distributed table reached by a shard's insert is checked like any other table,
-- so the requirement is never forwarded past the first hop.

DROP TABLE IF EXISTS insert_expected_nested_outer;
DROP TABLE IF EXISTS insert_expected_nested_inner;
DROP TABLE IF EXISTS insert_expected_nested;

CREATE TABLE insert_expected_nested (x UInt8) ENGINE = Memory;
CREATE TABLE insert_expected_nested_inner AS insert_expected_nested ENGINE = Distributed(test_shard_localhost, currentDatabase(), insert_expected_nested);
CREATE TABLE insert_expected_nested_outer AS insert_expected_nested ENGINE = Distributed(test_shard_localhost, currentDatabase(), insert_expected_nested_inner);

-- The table named forwards the requirement; the inner one is what the shard resolves, so it is refused.
INSERT INTO insert_expected_nested_outer SETTINGS insert_expected_table_engine = 'Memory', distributed_foreground_insert = 1 VALUES (1); -- { serverError UNEXPECTED_TABLE_ENGINE }
INSERT INTO insert_expected_nested_outer SETTINGS insert_expected_table_engine = 'Memory', distributed_foreground_insert = 1, prefer_localhost_replica = 0 VALUES (2); -- { serverError UNEXPECTED_TABLE_ENGINE }

-- Without a requirement both hops write, and one hop still forwards it.
INSERT INTO insert_expected_nested_outer SETTINGS distributed_foreground_insert = 1 VALUES (3);
INSERT INTO insert_expected_nested_inner SETTINGS insert_expected_table_engine = 'Memory', distributed_foreground_insert = 1 VALUES (4);
INSERT INTO insert_expected_nested_inner SETTINGS insert_expected_table_engine = 'Memory', distributed_foreground_insert = 1, prefer_localhost_replica = 0 VALUES (5);

SELECT * FROM insert_expected_nested ORDER BY x;

DROP TABLE insert_expected_nested_outer;
DROP TABLE insert_expected_nested_inner;
DROP TABLE insert_expected_nested;

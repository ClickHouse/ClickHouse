-- Tags: distributed

-- https://github.com/ClickHouse/ClickHouse/issues/75699
-- Logical error "Column ... query tree node does not have valid source node"
-- when joining a Merge table (that wraps a Distributed table) with another table.

DROP TABLE IF EXISTS test_75699_a;
DROP TABLE IF EXISTS test_75699_aa;
DROP TABLE IF EXISTS test_75699_b;
DROP TABLE IF EXISTS test_75699_m;

CREATE TABLE test_75699_a (key UInt32) ENGINE = MergeTree ORDER BY key;
CREATE TABLE test_75699_b (key UInt32, ID UInt32) ENGINE = MergeTree ORDER BY key;
-- The regex 'test_75699_a' matches both test_75699_a (MergeTree) and test_75699_aa (Distributed)
CREATE TABLE test_75699_aa (key UInt32) ENGINE = Distributed(test_shard_localhost, currentDatabase(), test_75699_a, key);
CREATE TABLE test_75699_m (key UInt32) ENGINE = Merge(currentDatabase(), 'test_75699_a');

INSERT INTO test_75699_a VALUES (0);
INSERT INTO test_75699_b VALUES (0, 1), (42, 1);

SELECT * FROM test_75699_m INNER JOIN test_75699_b USING(key) WHERE ID = 1;

DROP TABLE test_75699_a;
DROP TABLE test_75699_aa;
DROP TABLE test_75699_b;
DROP TABLE test_75699_m;

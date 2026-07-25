-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: the test uses explicit ZooKeeper paths in the engine arguments
-- and inspects the raw table metadata node written by `ReplicatedMergeTree`.
-- Tag no-shared-merge-tree: same reason.

-- A reverse sorting key written with redundant parentheses (`ORDER BY (a) DESC`) keeps the
-- `parenthesized` flag on the expression wrapped inside `ASTStorageOrderByElement`. The metadata
-- serialized to ZooKeeper must contain the canonical form `a DESC` without the parentheses,
-- because older server versions compare this field as text against their canonical local form
-- and would fail to join the table with METADATA_MISMATCH.
-- Related: https://github.com/ClickHouse/ClickHouse/pull/110833

DROP TABLE IF EXISTS t_reverse_parens;
DROP TABLE IF EXISTS t_reverse_parens_list;

CREATE TABLE t_reverse_parens (a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_reverse_parens', 'r1')
ORDER BY (a) DESC;

SELECT extract(value, 'primary key: [^\n]*') FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/t_reverse_parens' AND name = 'metadata';

CREATE TABLE t_reverse_parens_list (a UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_reverse_parens_list', 'r1')
ORDER BY ((a) DESC, b);

SELECT extract(value, 'primary key: [^\n]*') FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/t_reverse_parens_list' AND name = 'metadata';

DROP TABLE t_reverse_parens;
DROP TABLE t_reverse_parens_list;

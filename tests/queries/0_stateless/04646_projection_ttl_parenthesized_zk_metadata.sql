-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: the test uses explicit ZooKeeper paths in the engine arguments
-- and inspects the raw table metadata node written by `ReplicatedMergeTree`.
-- Tag no-shared-merge-tree: same reason.

-- Redundant parentheses written by the user are kept in the AST since #92340, including deep
-- inside a projection definition (`WITH (b + 1) AS y SELECT (a) AS x ... GROUP BY (a)`) and
-- inside a TTL element (`GROUP BY (b) SET c = max((c)) WHERE (a > 0)`). The metadata serialized
-- to ZooKeeper must contain the canonical form without them, because older server versions
-- compare these fields as text against their canonical local form and would otherwise refuse to
-- join the table with METADATA_MISMATCH.
-- Related: https://github.com/ClickHouse/ClickHouse/pull/110833

DROP TABLE IF EXISTS t_paren_projection;
DROP TABLE IF EXISTS t_paren_ttl;
DROP TABLE IF EXISTS t_paren_ttl_where;

CREATE TABLE t_paren_projection (a UInt32, b UInt32, c UInt32,
    PROJECTION p (WITH (b + 1) AS y SELECT (a) AS x, sum(y) GROUP BY (a)),
    INDEX ix ((b) * c) TYPE minmax GRANULARITY 1,
    CONSTRAINT cc CHECK (a) > 0)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_paren_projection', 'r1')
ORDER BY a;

SELECT extract(value, 'projections: [^\n]*'), extract(value, 'indices: [^\n]*'), extract(value, 'constraints: [^\n]*')
FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/t_paren_projection' AND name = 'metadata';

CREATE TABLE t_paren_ttl (a UInt32, b UInt32, c UInt32, d Date)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_paren_ttl', 'r1')
ORDER BY (a, b)
TTL (d) + INTERVAL 10 YEAR GROUP BY (a), (b) SET c = max((c));

SELECT extract(value, 'ttl: [^\n]*') FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/t_paren_ttl' AND name = 'metadata';

CREATE TABLE t_paren_ttl_where (a UInt32, b UInt32, d Date)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_paren_ttl_where', 'r1')
ORDER BY a
TTL (d) + INTERVAL 10 YEAR DELETE WHERE (a) > 0;

SELECT extract(value, 'ttl: [^\n]*') FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/t_paren_ttl_where' AND name = 'metadata';

DROP TABLE t_paren_projection;
DROP TABLE t_paren_ttl;
DROP TABLE t_paren_ttl_where;

-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: the test uses explicit ZooKeeper paths in the engine arguments
-- and inspects the raw table metadata node written by `ReplicatedMergeTree`.
-- Tag no-shared-merge-tree: same reason.

-- Same as `04646_projection_ttl_parenthesized_zk_metadata`, but for the `ALTER` write path:
-- an `ALTER` fills the changed fields of the metadata node itself, and it must use the same
-- canonical serialization as table creation. Otherwise the redundant parentheses the user wrote
-- (kept in the AST since #92340) end up in `/metadata`, and an older replica, which compares
-- these fields as text against its canonical local form, refuses to apply the `ALTER` entry or
-- to join the table with METADATA_MISMATCH.
-- Related: https://github.com/ClickHouse/ClickHouse/pull/110833

DROP TABLE IF EXISTS t_paren_alter;

CREATE TABLE t_paren_alter (a UInt32, b UInt32, c UInt32, d Date)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_paren_alter', 'r1')
ORDER BY a;

ALTER TABLE t_paren_alter
    ADD INDEX ix ((b) * c) TYPE minmax GRANULARITY 1,
    ADD PROJECTION p (WITH (b + 1) AS y SELECT (a) AS x, sum(y) GROUP BY (a)),
    ADD CONSTRAINT cc CHECK (a) > 0;

-- A `TTL` element list is parsed greedily up to the end of the query, so this cannot be a part
-- of the `ALTER` above.
ALTER TABLE t_paren_alter MODIFY TTL (d) + INTERVAL 10 YEAR GROUP BY (a) SET c = max((c));

SELECT
    extract(value, 'ttl: [^\n]*'),
    extract(value, 'indices: [^\n]*'),
    extract(value, 'projections: [^\n]*'),
    extract(value, 'constraints: [^\n]*')
FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/t_paren_alter' AND name = 'metadata';

-- The keys are filled by the same code path. The sorting key can only be extended with a newly
-- added column, and the sampling expression must be a part of the primary key.
ALTER TABLE t_paren_alter ADD COLUMN e UInt32, MODIFY ORDER BY (a, (e));
ALTER TABLE t_paren_alter MODIFY SAMPLE BY (a);

SELECT
    extract(value, 'sorting key: [^\n]*'),
    extract(value, 'primary key: [^\n]*'),
    extract(value, 'sampling expression: [^\n]*')
FROM system.zookeeper
WHERE path = '/clickhouse/' || currentDatabase() || '/t_paren_alter' AND name = 'metadata';

DROP TABLE t_paren_alter;

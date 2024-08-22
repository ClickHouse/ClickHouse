-- https://github.com/ClickHouse/ClickHouse/issues/23104
SET enable_analyzer=1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.base
(
`id` UInt64,
`id2` UInt64,
`d` UInt64,
`value` UInt64
)
ENGINE=MergeTree()
PARTITION BY d
ORDER BY (id,id2,d);

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.derived1
(
    `id1` UInt64,
    `d1` UInt64,
    `value1` UInt64
)
ENGINE = MergeTree()
PARTITION BY d1
ORDER BY (id1, d1);

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.derived2
(
    `id2` UInt64,
    `d2` UInt64,
    `value2` UInt64
)
ENGINE = MergeTree()
PARTITION BY d2
ORDER BY (id2, d2);

SELECT
    base.id AS `base.id`,
    derived2.id2 AS `derived2.id2`,
    derived2.value2 AS `derived2.value2`,
    derived1.value1 AS `derived1.value1`
FROM {CLICKHOUSE_DATABASE:Identifier}.base AS base
LEFT JOIN {CLICKHOUSE_DATABASE:Identifier}.derived2 AS derived2 ON base.id2 = derived2.id2
LEFT JOIN {CLICKHOUSE_DATABASE:Identifier}.derived1 AS derived1 ON base.id = derived1.id1;

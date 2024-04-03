-- https://github.com/ClickHouse/ClickHouse/issues/23104
DROP DATABASE IF EXISTS test_03053;
CREATE DATABASE test_03053;

CREATE TABLE test_03053.base
(
`id` UInt64,
`id2` UInt64,
`d` UInt64,
`value` UInt64
)
ENGINE=MergeTree()
PARTITION BY d
ORDER BY (id,id2,d);

CREATE TABLE test_03053.derived1
(
    `id1` UInt64,
    `d1` UInt64,
    `value1` UInt64
)
ENGINE = MergeTree()
PARTITION BY d1
ORDER BY (id1, d1);

CREATE TABLE test_03053.derived2
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
FROM test_03053.base AS base
LEFT JOIN test_03053.derived2 AS derived2 ON base.id2 = derived2.id2
LEFT JOIN test_03053.derived1 AS derived1 ON base.id = derived1.id1;

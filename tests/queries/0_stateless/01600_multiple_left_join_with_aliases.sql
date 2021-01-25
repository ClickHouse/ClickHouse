drop database if exists test_01600;
create database test_01600;

CREATE TABLE test_01600.base
(
`id` UInt64,
`id2` UInt64,
`d` UInt64,
`value` UInt64
)
ENGINE=MergeTree()
PARTITION BY d
ORDER BY (id,id2,d);

CREATE TABLE test_01600.derived1
(
    `id1` UInt64,
    `d1` UInt64,
    `value1` UInt64
)
ENGINE = MergeTree()
PARTITION BY d1
ORDER BY (id1, d1)
;

CREATE TABLE test_01600.derived2
(
    `id2` UInt64,
    `d2` UInt64,
    `value2` UInt64
)
ENGINE = MergeTree()
PARTITION BY d2
ORDER BY (id2, d2)
;

select 
base.id as `base.id`,
derived2.value2 as `derived2.value2`,
derived1.value1 as `derived1.value1`
from test_01600.base as base 
left join test_01600.derived2 as derived2 on base.id2 = derived2.id2
left join test_01600.derived1 as derived1 on base.id = derived1.id1;


SELECT
    base.id AS `base.id`,
    derived1.value1 AS `derived1.value1`
FROM test_01600.base AS base
LEFT JOIN test_01600.derived1 AS derived1 ON base.id = derived1.id1;

drop database test_01600;

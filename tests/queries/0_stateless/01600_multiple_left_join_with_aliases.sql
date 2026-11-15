CREATE TABLE base
(
`id` UInt64,
`id2` UInt64,
`d` UInt64,
`value` UInt64
)
ENGINE=MergeTree()
PARTITION BY d
ORDER BY (id,id2,d);

CREATE TABLE derived1
(
    `id1` UInt64,
    `d1` UInt64,
    `value1` UInt64
)
ENGINE = MergeTree()
PARTITION BY d1
ORDER BY (id1, d1)
;

CREATE TABLE derived2
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
from base as base
left join derived2 as derived2 on base.id2 = derived2.id2
left join derived1 as derived1 on base.id = derived1.id1;


SELECT
    base.id AS `base.id`,
    derived1.value1 AS `derived1.value1`
FROM base AS base
LEFT JOIN derived1 AS derived1 ON base.id = derived1.id1;

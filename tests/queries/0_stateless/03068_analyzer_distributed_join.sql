-- Tags: no-replicated-database, shard
-- Closes: https://github.com/ClickHouse/ClickHouse/issues/6571

SET enable_analyzer=1;
CREATE TABLE LINEITEM_shard ON CLUSTER test_shard_localhost
(
    L_ORDERKEY UInt64,
    L_COMMITDATE UInt32,
    L_RECEIPTDATE UInt32
)
ENGINE = MergeTree()
ORDER BY L_ORDERKEY;

CREATE TABLE LINEITEM AS LINEITEM_shard
ENGINE = Distributed('test_shard_localhost', currentDatabase(), LINEITEM_shard, rand());

CREATE TABLE ORDERS_shard ON CLUSTER test_shard_localhost
(
    O_ORDERKEY UInt64,
    O_ORDERPRIORITY UInt32
)
ENGINE = MergeTree()
ORDER BY O_ORDERKEY;

CREATE TABLE ORDERS AS ORDERS_shard
ENGINE = Distributed('test_shard_localhost', currentDatabase(), ORDERS_shard, rand());

SET joined_subquery_requires_alias=0;

select
	O_ORDERPRIORITY,
	count(*) as order_count
from ORDERS JOIN (
	select L_ORDERKEY
	from
	LINEITEM_shard
	group by L_ORDERKEY
	having any(L_COMMITDATE < L_RECEIPTDATE)
) on O_ORDERKEY=L_ORDERKEY
group by O_ORDERPRIORITY
order by O_ORDERPRIORITY
limit 1;

SET joined_subquery_requires_alias=1;

select
	O_ORDERPRIORITY,
	count(*) as order_count
from ORDERS JOIN (
	select L_ORDERKEY
	from
	LINEITEM_shard
	group by L_ORDERKEY
	having any(L_COMMITDATE < L_RECEIPTDATE)
) AS x on O_ORDERKEY=L_ORDERKEY
group by O_ORDERPRIORITY
order by O_ORDERPRIORITY
limit 1;

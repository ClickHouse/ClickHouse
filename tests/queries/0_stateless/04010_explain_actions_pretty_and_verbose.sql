CREATE TABLE customer
(
    c_custkey     Int32,
    c_mktsegment  String
)
ENGINE = MergeTree
ORDER BY (c_custkey);

CREATE TABLE orders
(
    o_orderkey      Int32,
    o_custkey       Int32,
    o_orderdate     Date,
    o_shippriority  Int32
)
ENGINE = MergeTree
ORDER BY (o_orderkey);

CREATE TABLE lineitem
(
    l_orderkey       Int32,
    l_extendedprice  Decimal(15,2),
    l_discount       Decimal(15,2),
    l_shipdate       Date
)
ENGINE = MergeTree
ORDER BY (l_orderkey);

EXPLAIN actions=1, verbose=0 SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;

EXPLAIN actions=1, pretty=1 SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;

DROP TABLE lineitem;
DROP TABLE customer;
DROP TABLE orders;
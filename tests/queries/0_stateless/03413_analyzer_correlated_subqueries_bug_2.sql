set enable_analyzer = 1;
set allow_experimental_correlated_subqueries = 1;

CREATE TABLE orders  (
    o_orderkey       Int32,
    o_custkey        Int32,
    o_orderstatus    String,
    o_totalprice     Decimal(15,2),
    o_orderdate      Date,
    o_orderpriority  String,
    o_clerk          String,
    o_shippriority   Int32,
    o_comment        String)
ORDER BY (o_orderkey);

INSERT INTO orders SELECT * FROM generateRandom() LIMIT 10;

CREATE TABLE lineitem (
    l_orderkey       Int32,
    l_partkey        Int32,
    l_suppkey        Int32,
    l_linenumber     Int32,
    l_quantity       Decimal(15,2),
    l_extendedprice  Decimal(15,2),
    l_discount       Decimal(15,2),
    l_tax            Decimal(15,2),
    l_returnflag     String,
    l_linestatus     String,
    l_shipdate       Date,
    l_commitdate     Date,
    l_receiptdate    Date,
    l_shipinstruct   String,
    l_shipmode       String,
    l_comment        String)
ORDER BY (l_orderkey, l_linenumber);

SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
(
    SELECT
        o_orderpriority,
        o_orderkey
    FROM orders
    WHERE (o_orderdate >= toDate('1993-07-01')) AND (o_orderdate < (toDate('1993-07-01') + toIntervalMonth('3')))
)
WHERE exists((
    SELECT l_orderkey
    FROM lineitem
    WHERE (l_orderkey = o_orderkey) AND (l_commitdate < l_receiptdate)
))
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC
FORMAT Null;

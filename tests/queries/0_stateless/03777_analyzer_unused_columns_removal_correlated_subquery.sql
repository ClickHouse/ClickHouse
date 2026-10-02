SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET correlated_subqueries_substitute_equivalent_expressions = 0;

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
INSERT INTO lineitem SELECT * FROM generateRandom() LIMIT 1;

CREATE TABLE part (
    p_partkey     Int32,
    p_name        String,
    p_mfgr        String,
    p_brand       String,
    p_type        String,
    p_size        Int32,
    p_container   String,
    p_retailprice Decimal(15,2),
    p_comment     String)
ORDER BY (p_partkey);
INSERT INTO part SELECT * FROM generateRandom() LIMIT 1;


EXPLAIN QUERY TREE
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    (SELECT * FROM lineitem, part WHERE p_partkey = l_partkey) AS lp
WHERE
    l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = lp.p_partkey
    );

SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    (SELECT * FROM lineitem, part WHERE p_partkey = l_partkey) AS lp
WHERE
    l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = lp.p_partkey
    )
FORMAT Null;

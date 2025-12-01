-- SET param__internal_join_table_stat_hints = '{
--     "lineitem": { "cardinality": 1000, "distinct_keys": { "l_extendedprice": 100, "l_partkey": 1000, "l_quantity": 100 } },
--     "part": { "cardinality": 10, "distinct_keys": { "p_partkey": 1, "p_name": 100, "p_brand": 10 } }
-- }';
SET query_plan_optimize_join_order_limit = 0;

SET allow_statistics_optimize = 0;

SET correlated_subqueries_substitute_equivalent_expressions = 0;
SET correlated_subqueries_use_in_memory_buffer = 1;

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

EXPLAIN actions = 1
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem,
    part
WHERE
    p_partkey = l_partkey
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem
        WHERE
            l_partkey = p_partkey
    );

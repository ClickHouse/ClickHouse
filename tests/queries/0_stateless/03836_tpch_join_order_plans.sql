-- Verifies join order and distributed execution strategies for all TPC-H queries
-- using SF100 cardinalities injected via `_internal_join_table_stat_hints`.
-- Tables are empty but correctly schemed; a sentinel row per table prevents
-- 0-row short-circuits.  The Cascades optimizer with `make_distributed_plan=1`
-- produces a distributed plan: queries with large fact tables may use
-- `BroadcastExchange` or `ShuffleExchange`, while queries with only small
-- dimension tables may use `Local HashJoin` directly with `ReadFromMergeTree`.
-- Three queries (Q11, Q15, Q22) have non-correlated scalar subqueries evaluated
-- at analysis time; these are planned without cascades/distributed to avoid
-- the "Stateless worker client" restriction at that phase.

DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS lineitem;

CREATE TABLE region (
    r_regionkey Int32, r_name String, r_comment String
) ENGINE = MergeTree() ORDER BY r_regionkey;

CREATE TABLE nation (
    n_nationkey Int32, n_name String, n_regionkey Int32, n_comment String
) ENGINE = MergeTree() ORDER BY n_nationkey;

CREATE TABLE part (
    p_partkey Int32, p_name String, p_mfgr String, p_brand String,
    p_type String, p_size Int32, p_container String,
    p_retailprice Decimal(15,2), p_comment String
) ENGINE = MergeTree() ORDER BY p_partkey;

CREATE TABLE supplier (
    s_suppkey Int32, s_name String, s_address String, s_nationkey Int32,
    s_phone String, s_acctbal Decimal(15,2), s_comment String
) ENGINE = MergeTree() ORDER BY s_suppkey;

CREATE TABLE partsupp (
    ps_partkey Int32, ps_suppkey Int32, ps_availqty Int32,
    ps_supplycost Decimal(15,2), ps_comment String
) ENGINE = MergeTree() ORDER BY (ps_partkey, ps_suppkey);

CREATE TABLE customer (
    c_custkey Int32, c_name String, c_address String, c_nationkey Int32,
    c_phone String, c_acctbal Decimal(15,2), c_mktsegment String, c_comment String
) ENGINE = MergeTree() ORDER BY c_custkey;

CREATE TABLE orders (
    o_orderkey Int32, o_custkey Int32, o_orderstatus String,
    o_totalprice Decimal(15,2), o_orderdate Date, o_orderpriority String,
    o_clerk String, o_shippriority Int32, o_comment String
) ENGINE = MergeTree() ORDER BY o_orderkey;

CREATE TABLE lineitem (
    l_orderkey Int32, l_partkey Int32, l_suppkey Int32, l_linenumber Int32,
    l_quantity Decimal(15,2), l_extendedprice Decimal(15,2), l_discount Decimal(15,2),
    l_tax Decimal(15,2), l_returnflag String, l_linestatus String,
    l_shipdate Date, l_commitdate Date, l_receiptdate Date,
    l_shipinstruct String, l_shipmode String, l_comment String
) ENGINE = MergeTree() ORDER BY (l_orderkey, l_linenumber);

-- One sentinel row per table prevents 0-row short-circuit optimizations.
INSERT INTO region    VALUES (1, 'A', '');
INSERT INTO nation    VALUES (1, 'A', 1, '');
INSERT INTO part      VALUES (1, 'a', 'M', 'B', 'T', 1, 'C', 1.0, '');
INSERT INTO supplier  VALUES (1, 'A', 'A', 1, '0', 1.0, '');
INSERT INTO partsupp  VALUES (1, 1, 1, 1.0, '');
INSERT INTO customer  VALUES (1, 'A', 'A', 1, '0', 1.0, 'B', '');
INSERT INTO orders    VALUES (1, 1, 'O', 1.0, '1994-01-01', '1-URGENT', 'C1', 0, '');
INSERT INTO lineitem  VALUES (1, 1, 1, 1, 1.0, 1.0, 0.0, 0.0, 'N', 'O', '1994-02-01', '1994-01-15', '1994-02-05', 'DELIVER IN PERSON', 'SHIP', '');

SET allow_experimental_analyzer = 1;
SET enable_join_runtime_filters = 0;
SET use_join_disjunctions_push_down = 1;
SET query_plan_optimize_join_order_limit = 10;
SET allow_statistic_optimize = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsize,greedy';
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET distributed_plan_execute_locally = 0;
SET enable_cascades_optimizer = 1;
SET rewrite_in_to_join = 1;
SET correlated_subqueries_use_in_memory_buffer = 0;
SET allow_experimental_correlated_subqueries = 1;
SET send_logs_level = 'error';

-- SF100 cardinalities and key column NDVs for all TPC-H tables.
SET param__internal_join_table_stat_hints = '{
    "lineitem":  { "cardinality": 600037902,  "distinct_keys": { "l_orderkey": 150000000, "l_partkey": 20000000, "l_suppkey": 1000000, "l_linenumber": 7, "l_returnflag": 3, "l_linestatus": 2, "l_shipdate": 2526, "l_commitdate": 2466, "l_receiptdate": 2554, "l_quantity": 50, "l_discount": 11, "l_shipmode": 7, "l_shipinstruct": 4 } },
    "orders":    { "cardinality": 150000000,  "distinct_keys": { "o_orderkey": 150000000, "o_custkey": 15000000, "o_orderdate": 2406, "o_orderstatus": 3, "o_orderpriority": 5, "o_clerk": 1000 } },
    "customer":  { "cardinality": 15000000,   "distinct_keys": { "c_custkey": 15000000, "c_nationkey": 25, "c_mktsegment": 5, "c_acctbal": 14975000, "c_phone": 14999997 } },
    "part":      { "cardinality": 20000000,   "distinct_keys": { "p_partkey": 20000000, "p_type": 150, "p_brand": 25, "p_size": 50, "p_container": 40, "p_name": 19999999 } },
    "supplier":  { "cardinality": 1000000,    "distinct_keys": { "s_suppkey": 1000000, "s_nationkey": 25, "s_acctbal": 999990 } },
    "partsupp":  { "cardinality": 80000000,   "distinct_keys": { "ps_partkey": 20000000, "ps_suppkey": 1000000, "ps_availqty": 9999, "ps_supplycost": 99865 } },
    "nation":    { "cardinality": 25,         "distinct_keys": { "n_nationkey": 25, "n_regionkey": 5, "n_name": 25 } },
    "region":    { "cardinality": 5,          "distinct_keys": { "r_regionkey": 5, "r_name": 5 } }
}';

-- Q01: Pricing summary (single table, no joins)
SELECT '-- Q01';
EXPLAIN
SELECT l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice),
    sum(l_extendedprice * (1 - l_discount)), sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
    avg(l_quantity), avg(l_extendedprice), avg(l_discount), count()
FROM lineitem WHERE l_shipdate <= '1998-09-02'
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;

-- Q02: Minimum cost supplier (5-table join + correlated subquery)
SELECT '-- Q02';
EXPLAIN
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15
    AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region
        WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE')
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;

-- Q03: Shipping priority (customer, orders, lineitem)
SELECT '-- Q03';
EXPLAIN
SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
    AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;

-- Q04: Order priority (orders + EXISTS subquery on lineitem)
SELECT '-- Q04';
EXPLAIN
SELECT o_orderpriority, count() AS order_count
FROM orders
WHERE o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01'
    AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority ORDER BY o_orderpriority;

-- Q05: Local supplier volume (customer, orders, lineitem, supplier, nation, region)
SELECT '-- Q05';
EXPLAIN
SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
    AND r_name = 'ASIA' AND o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01'
GROUP BY n_name ORDER BY revenue DESC;

-- Q06: Forecasting revenue (single table, no joins)
SELECT '-- Q06';
EXPLAIN
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;

-- Q07: Volume shipping (supplier, lineitem, orders, customer, nation n1, nation n2)
SELECT '-- Q07';
EXPLAIN
SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation,
    toYear(l_shipdate) AS l_year, sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
    AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
    AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year;

-- Q08: National market share (part, supplier, lineitem, orders, customer, nation n1, nation n2, region)
SELECT '-- Q08';
EXPLAIN
SELECT o_year,
    sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share
FROM (SELECT toYear(o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation
    FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31' AND p_type = 'ECONOMY ANODIZED STEEL'
    ) AS all_nations
GROUP BY o_year ORDER BY o_year;

-- Q09: Product type profit measure (part, supplier, lineitem, partsupp, orders, nation)
SELECT '-- Q09';
EXPLAIN
SELECT n_name AS nation, toYear(o_orderdate) AS o_year,
    sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS amount
FROM part, supplier, lineitem, partsupp, orders, nation
WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
    AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
GROUP BY nation, o_year ORDER BY nation, o_year DESC;

-- Q10: Returned item reporting (customer, orders, lineitem, nation)
SELECT '-- Q10';
EXPLAIN
SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= '1993-10-01'
    AND o_orderdate < '1994-01-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC LIMIT 20;

-- Q11: Important stock identification (partsupp, supplier, nation)
-- Non-correlated scalar subquery in HAVING is evaluated at analysis time;
-- cascades + distributed plan together cause "Stateless worker client" error,
-- so we disable both for this query only.
SELECT '-- Q11';
EXPLAIN
SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING sum(ps_supplycost * ps_availqty) > (
    SELECT sum(ps_supplycost * ps_availqty) * 0.0001
    FROM partsupp, supplier, nation
    WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY')
ORDER BY value DESC
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- Q12: Shipping modes and order priority (orders, lineitem)
SELECT '-- Q12';
EXPLAIN
SELECT l_shipmode,
    sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
    sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
    AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'
GROUP BY l_shipmode ORDER BY l_shipmode;

-- Q13: Customer distribution (LEFT OUTER JOIN customer, orders)
SELECT '-- Q13';
EXPLAIN
SELECT c_count, count() AS custdist
FROM (SELECT c_custkey, count(o_orderkey) AS c_count
    FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey
        AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey) AS c_orders
GROUP BY c_count ORDER BY custdist DESC, c_count DESC
SETTINGS join_use_nulls = 1;

-- Q14: Promotion effect (lineitem, part)
SELECT '-- Q14';
EXPLAIN
SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
    / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem, part
WHERE l_partkey = p_partkey AND l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01';

-- Q15: Top supplier (view + supplier join)
-- Non-correlated scalar subquery (max(total_revenue)) is evaluated at analysis time;
-- disable cascades + distributed plan for this query only.
SELECT '-- Q15';
DROP VIEW IF EXISTS revenue0;
CREATE VIEW revenue0 AS
    SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM lineitem WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01'
    GROUP BY l_suppkey;
EXPLAIN
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM supplier, revenue0
WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM revenue0)
ORDER BY s_suppkey
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;
DROP VIEW revenue0;

-- Q16: Parts/supplier relationship (partsupp, part + NOT IN subquery)
SELECT '-- Q16';
EXPLAIN
SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%')
GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;

-- Q17: Small-quantity orders (lineitem, part + correlated subquery)
SELECT '-- Q17';
EXPLAIN
SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem, part
WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX'
    AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey);

-- Q18: Large volume customer (customer, orders, lineitem + IN subquery)
SELECT '-- Q18';
EXPLAIN
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
    AND c_custkey = o_custkey AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;

-- Q19: Discounted revenue (lineitem, part)
SELECT '-- Q19';
EXPLAIN
SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem, part
WHERE p_partkey = l_partkey
    AND ((p_brand = 'Brand#12' AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG')
            AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5
            AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')
        OR (p_brand = 'Brand#23' AND p_container IN ('MED BAG','MED BOX','MED PKG','MED PACK')
            AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10
            AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')
        OR (p_brand = 'Brand#34' AND p_container IN ('LG CASE','LG BOX','LG PACK','LG PKG')
            AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15
            AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'));

-- Q20: Potential part promotion (supplier, nation + nested IN subqueries)
SELECT '-- Q20';
EXPLAIN
SELECT s_name, s_address
FROM supplier, nation
WHERE s_suppkey IN (
    SELECT ps_suppkey FROM partsupp
    WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
        AND ps_availqty > (
            SELECT 0.5 * sum(l_quantity) FROM lineitem
            WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'))
    AND s_nationkey = n_nationkey AND n_name = 'CANADA'
ORDER BY s_name;

-- Q21: Suppliers who kept orders waiting (supplier, lineitem l1, orders, nation + EXISTS subqueries)
SELECT '-- Q21';
EXPLAIN
SELECT s_name, count() AS numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
    AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey
        AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
    AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100;

-- Q22: Global sales opportunity (customer + NOT EXISTS on orders)
-- Non-correlated scalar subquery (avg(c_acctbal)) is evaluated at analysis time;
-- disable cascades + distributed plan for this query only.
SELECT '-- Q22';
EXPLAIN
SELECT cntrycode, count() AS numcust, sum(c_acctbal) AS totacctbal
FROM (SELECT substring(c_phone, 1, 2) AS cntrycode, c_acctbal
    FROM customer
    WHERE substring(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')
        AND c_acctbal > (SELECT avg(c_acctbal) FROM customer
            WHERE c_acctbal > 0 AND substring(c_phone, 1, 2) IN ('13','31','23','29','30','18','17'))
        AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)
    ) AS custsale
GROUP BY cntrycode ORDER BY cntrycode
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE lineitem;
DROP TABLE orders;
DROP TABLE customer;
DROP TABLE partsupp;
DROP TABLE supplier;
DROP TABLE part;
DROP TABLE nation;
DROP TABLE region;

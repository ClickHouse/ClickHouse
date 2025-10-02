DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS lineitem;

CREATE TABLE nation (
    n_nationkey  Int32,
    n_name       String,
    n_regionkey  Int32,
    n_comment    String)
ORDER BY (n_nationkey);

CREATE TABLE region (
    r_regionkey  Int32,
    r_name       String,
    r_comment    String)
ORDER BY (r_regionkey);

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

CREATE TABLE supplier (
    s_suppkey     Int32,
    s_name        String,
    s_address     String,
    s_nationkey   Int32,
    s_phone       String,
    s_acctbal     Decimal(15,2),
    s_comment     String)
ORDER BY (s_suppkey);

CREATE TABLE partsupp (
    ps_partkey     Int32,
    ps_suppkey     Int32,
    ps_availqty    Int32,
    ps_supplycost  Decimal(15,2),
    ps_comment     String)
ORDER BY (ps_partkey, ps_suppkey);

CREATE TABLE customer (
    c_custkey     Int32,
    c_name        String,
    c_address     String,
    c_nationkey   Int32,
    c_phone       String,
    c_acctbal     Decimal(15,2),
    c_mktsegment  String,
    c_comment     String)
ORDER BY (c_custkey);

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
-- The following is an alternative order key which is not compliant with the official TPC-H rules but recommended by sec. 4.5 in
-- "Quantifying TPC-H Choke Points and Their Optimizations":
-- ORDER BY (o_orderdate, o_orderkey);

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
-- The following is an alternative order key which is not compliant with the official TPC-H rules but recommended by sec. 4.5 in
-- "Quantifying TPC-H Choke Points and Their Optimizations":
-- ORDER BY (l_shipdate, l_orderkey, l_linenumber);

INSERT INTO nation SELECT * FROM generateRandom() LIMIT 1;
INSERT INTO region SELECT * FROM generateRandom() LIMIT 1;
INSERT INTO part SELECT * FROM generateRandom() LIMIT 1;
INSERT INTO supplier SELECT * FROM generateRandom() LIMIT 1;
INSERT INTO partsupp SELECT * FROM generateRandom() LIMIT 1;
INSERT INTO customer SELECT * FROM generateRandom() LIMIT 1;
INSERT INTO orders SELECT * FROM generateRandom() LIMIT 1;
INSERT INTO lineitem SELECT * FROM generateRandom() LIMIT 1;

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1;

-- Q7
SELECT
  supp_nation,
  cust_nation,
  l_year,
  SUM(volume) AS revenue
FROM
  (
    SELECT
      n1.n_name AS supp_nation,
      n2.n_name AS cust_nation,
      l_year,
      volume
    FROM
      (
        SELECT
          l_suppkey,
          l_orderkey,
          EXTRACT(
            YEAR
            FROM
              l_shipdate
          ) AS l_year,
          SUM(l_extendedprice * (1 - l_discount)) AS volume
        FROM
          lineitem
        WHERE
          l_suppkey IN (
            SELECT
              s_suppkey
            FROM
              supplier
            WHERE
              s_nationkey IN (
                SELECT
                  n_nationkey
                FROM
                  nation
                WHERE
                  n_name = 'FRANCE'
                  OR n_name = 'GERMANY'
              )
          )
          AND l_orderkey IN (
            SELECT
              o_orderkey
            FROM
              orders
            WHERE
              o_custkey IN (
                SELECT
                  c_custkey
                FROM
                  customer
                WHERE
                  c_nationkey IN (
                    SELECT
                      n_nationkey
                    FROM
                      nation
                    WHERE
                      n_name = 'FRANCE'
                      OR n_name = 'GERMANY'
                  )
              )
          )
          AND l_shipdate BETWEEN DATE '1995-01-01'
          AND DATE '1996-12-31'
        GROUP BY
          l_year,
          l_suppkey,
          l_orderkey
      ) AS l,
      orders,
      supplier,
      customer,
      nation n1,
      nation n2
    WHERE
      s_suppkey = l_suppkey
      AND o_orderkey = l_orderkey
      AND c_custkey = o_custkey
      AND s_nationkey = n1.n_nationkey
      AND c_nationkey = n2.n_nationkey
      AND (
        (
          n1.n_name = 'FRANCE'
          AND n2.n_name = 'GERMANY'
        )
        OR (
          n1.n_name = 'GERMANY'
          AND n2.n_name = 'FRANCE'
        )
      )
      AND s_nationkey IN (
        SELECT
          n_nationkey
        FROM
          nation
        WHERE
          n_name = 'FRANCE'
          OR n_name = 'GERMANY'
      )
      AND o_custkey IN (
        SELECT
          c_custkey
        FROM
          customer
        WHERE
          c_nationkey IN (
            SELECT
              n_nationkey
            FROM
              nation
            WHERE
              n_name = 'FRANCE'
              OR n_name = 'GERMANY'
          )
      )
  ) AS shipping
GROUP BY
  supp_nation,
  cust_nation,
  l_year
ORDER BY
  supp_nation,
  cust_nation,
  l_year FORMAT NULL;

-- Q10
SELECT
  c_custkey,
  c_name,
  SUM(revenue) AS revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
FROM
  (
    SELECT
      l_orderkey,
      SUM(l_extendedprice * (1 - l_discount)) AS revenue
    FROM
      lineitem
    WHERE
      l_returnflag = 'R'
    GROUP BY
      l_orderkey
  ) AS l,
  orders,
  customer,
  nation
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= DATE '1993-10-01'
  AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
  AND c_nationkey = n_nationkey
GROUP BY
  c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
ORDER BY
  revenue desc
LIMIT
  20 FORMAT NULL;

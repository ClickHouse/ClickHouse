-- Types mapping (per TPC-H spec section 1.3.1):
--   Identifier         -> UInt32, covers the required >= 2,147,483,647 unique values
--   Integer            -> Int32, covers the required -2,147,483,646 to 2,147,483,647 range
--   Decimal            -> Decimal(12, 2), covers the required ±9,999,999,999.99 range
--   Fixed text, N      -> FixedString(N), because ClickHouse treats CHAR(N) as variable-length String
--   Variable text, N   -> String
--   Date               -> Date
--
-- The spec requires that the chosen type for each datatype definition is applied consistently across all columns, except for Identifier
-- columns: at SF > 300 some identifiers may exceed the 4-byte integer range, and the spec permits using a wider type for only that column.
-- This schema has not been tested at SF > 300; some Identifier columns may need to be upgraded to a wider type.
--
-- Primary keys are per section 1.4.1 of the TPC-H specification.

CREATE TABLE nation (
    n_nationkey  UInt32,
    n_name       FixedString(25),
    n_regionkey  UInt32,
    n_comment    String)
PRIMARY KEY (n_nationkey);

CREATE TABLE region (
    r_regionkey  UInt32,
    r_name       FixedString(25),
    r_comment    String)
PRIMARY KEY (r_regionkey);

CREATE TABLE part (
    p_partkey     UInt32,
    p_name        String,
    p_mfgr        FixedString(25),
    p_brand       FixedString(10),
    p_type        String,
    p_size        Int32,
    p_container   FixedString(10),
    p_retailprice Decimal(12,2),
    p_comment     String)
PRIMARY KEY (p_partkey);

CREATE TABLE supplier (
    s_suppkey     UInt32,
    s_name        FixedString(25),
    s_address     String,
    s_nationkey   UInt32,
    s_phone       FixedString(15),
    s_acctbal     Decimal(12,2),
    s_comment     String)
PRIMARY KEY (s_suppkey);

CREATE TABLE partsupp (
    ps_partkey     UInt32,
    ps_suppkey     UInt32,
    ps_availqty    Int32,
    ps_supplycost  Decimal(12,2),
    ps_comment     String)
PRIMARY KEY (ps_partkey, ps_suppkey);

CREATE TABLE customer (
    c_custkey     UInt32,
    c_name        String,
    c_address     String,
    c_nationkey   UInt32,
    c_phone       FixedString(15),
    c_acctbal     Decimal(12,2),
    c_mktsegment  FixedString(10),
    c_comment     String)
PRIMARY KEY (c_custkey);

CREATE TABLE orders (
    o_orderkey       UInt32,
    o_custkey        UInt32,
    o_orderstatus    FixedString(1),
    o_totalprice     Decimal(12,2),
    o_orderdate      Date,
    o_orderpriority  FixedString(15),
    o_clerk          FixedString(15),
    o_shippriority   Int32,
    o_comment        String)
PRIMARY KEY (o_orderkey);

CREATE TABLE lineitem (
    l_orderkey    UInt32,
    l_partkey     UInt32,
    l_suppkey     UInt32,
    l_linenumber  Int32,
    l_quantity    Decimal(12,2),
    l_extendedprice  Decimal(12,2),
    l_discount    Decimal(12,2),
    l_tax         Decimal(12,2),
    l_returnflag  FixedString(1),
    l_linestatus  FixedString(1),
    l_shipdate    Date,
    l_commitdate  Date,
    l_receiptdate Date,
    l_shipinstruct  FixedString(25),
    l_shipmode      FixedString(10),
    l_comment       String)
PRIMARY KEY (l_orderkey, l_linenumber);

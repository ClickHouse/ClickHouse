SET enable_analyzer = 1;

DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS lineorder;
DROP TABLE IF EXISTS date;

CREATE TABLE customer
(
        C_CUSTKEY       UInt32,
        C_NAME          String,
        C_ADDRESS       String,
        C_CITY          LowCardinality(String),
        C_NATION        LowCardinality(String),
        C_REGION        LowCardinality(String),
        C_PHONE         String,
        C_MKTSEGMENT    LowCardinality(String)
)
ENGINE = MergeTree ORDER BY (C_CUSTKEY);

CREATE TABLE lineorder
(
    LO_ORDERKEY             UInt32,
    LO_LINENUMBER           UInt8,
    LO_CUSTKEY              UInt32,
    LO_PARTKEY              UInt32,
    LO_SUPPKEY              UInt32,
    LO_ORDERDATE            Date,
    LO_ORDERPRIORITY        LowCardinality(String),
    LO_SHIPPRIORITY         UInt8,
    LO_QUANTITY             UInt8,
    LO_EXTENDEDPRICE        UInt32,
    LO_ORDTOTALPRICE        UInt32,
    LO_DISCOUNT             UInt8,
    LO_REVENUE              UInt32,
    LO_SUPPLYCOST           UInt32,
    LO_TAX                  UInt8,
    LO_COMMITDATE           Date,
    LO_SHIPMODE             LowCardinality(String)
)
ENGINE = MergeTree PARTITION BY toYear(LO_ORDERDATE) ORDER BY (LO_ORDERDATE, LO_ORDERKEY);

CREATE TABLE part
(
        P_PARTKEY       UInt32,
        P_NAME          String,
        P_MFGR          LowCardinality(String),
        P_CATEGORY      LowCardinality(String),
        P_BRAND         LowCardinality(String),
        P_COLOR         LowCardinality(String),
        P_TYPE          LowCardinality(String),
        P_SIZE          UInt8,
        P_CONTAINER     LowCardinality(String)
)
ENGINE = MergeTree ORDER BY P_PARTKEY;

CREATE TABLE supplier
(
        S_SUPPKEY       UInt32,
        S_NAME          String,
        S_ADDRESS       String,
        S_CITY          LowCardinality(String),
        S_NATION        LowCardinality(String),
        S_REGION        LowCardinality(String),
        S_PHONE         String
)
ENGINE = MergeTree ORDER BY S_SUPPKEY;

CREATE TABLE date
(
        D_DATEKEY            Date,
        D_DATE               FixedString(18),
        D_DAYOFWEEK          LowCardinality(String),
        D_MONTH              LowCardinality(String),
        D_YEAR               UInt16,
        D_YEARMONTHNUM       UInt32,
        D_YEARMONTH          LowCardinality(FixedString(7)),
        D_DAYNUMINWEEK       UInt8,
        D_DAYNUMINMONTH      UInt8,
        D_DAYNUMINYEAR       UInt16,
        D_MONTHNUMINYEAR     UInt8,
        D_WEEKNUMINYEAR      UInt8,
        D_SELLINGSEASON      String,
        D_LASTDAYINWEEKFL    UInt8,
        D_LASTDAYINMONTHFL   UInt8,
        D_HOLIDAYFL          UInt8,
        D_WEEKDAYFL          UInt8
)
ENGINE = MergeTree ORDER BY D_DATEKEY;

set cross_to_inner_join_rewrite = 2;

EXPLAIN QUERY TREE dump_ast=1
select D_YEARMONTHNUM, S_CITY, P_BRAND, sum(LO_REVENUE - LO_SUPPLYCOST) as profit
from date, customer, supplier, part, lineorder
where LO_CUSTKEY = C_CUSTKEY
  and LO_SUPPKEY = S_SUPPKEY
  and LO_PARTKEY = P_PARTKEY
  and LO_ORDERDATE = D_DATEKEY
  and S_NATION = 'UNITED KINGDOM'
  and P_CATEGORY = 'MFGR#21'
  and (LO_QUANTITY between 34 and 44)
  and (LO_ORDERDATE between toDate('1996-01-01') and toDate('1996-12-31'))
group by D_YEARMONTHNUM, S_CITY, P_BRAND
order by D_YEARMONTHNUM, S_CITY, P_BRAND;

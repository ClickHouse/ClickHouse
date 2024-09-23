---
slug: /en/getting-started/example-datasets/star-schema
sidebar_label: Star Schema Benchmark
description:  "Dataset based on the TPC-H dbgen source. The coding style and architecture follows the TPCH dbgen."
---

# Star Schema Benchmark (SSB, 2009)

The Star Schema Benchmark is roughly based on the TPC-H tables and queries but unlike TPC-H, it uses a star schema layout.
The bulk of the data sits in a gigantic fact table which is surrounded by multiple small dimension tables.
The queries joined the fact table with one or more dimension tables to apply filter criteria, e.g. `MONTH = 'JANUARY'`.

References:
[Star Schema Benchmark](https://cs.umb.edu/~poneil/StarSchemaB.pdf) (O'Neil et. al), 2009
[Variations of the Star Schema Benchmark to Test the Effects of Data Skew on Query Performance](https://doi.org/10.1145/2479871.2479927) (Rabl. et. al.), 2013



First, checkout the star schema benchmark repository and compile the data generator:
``` bash
$ git clone https://github.com/vadimtk/ssb-dbgen.git
$ cd ssb-dbgen
$ make
```

Then, generate the data. Parameter `-s` specifies the scale factor. For example, with `-s 100`, 600 million rows (67 GB) are generated.

``` bash
$ ./dbgen -s 1000 -T c
$ ./dbgen -s 1000 -T l
$ ./dbgen -s 1000 -T p
$ ./dbgen -s 1000 -T s
$ ./dbgen -s 1000 -T d
```

Now create tables in ClickHouse:

``` sql
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
```

The data can be imported as follows:

``` bash
$ clickhouse-client --query "INSERT INTO customer FORMAT CSV" < customer.tbl
$ clickhouse-client --query "INSERT INTO part FORMAT CSV" < part.tbl
$ clickhouse-client --query "INSERT INTO supplier FORMAT CSV" < supplier.tbl
$ clickhouse-client --query "INSERT INTO lineorder FORMAT CSV" < lineorder.tbl
$ clickhouse-client --query "INSERT INTO date FORMAT CSV" < date.tbl
```

In many use cases of ClickHouse, multiple tables are converted into a single denormalized flat table.
This step is optional, below queries are listed in their original form and in a format rewritten for the denormalized table.

``` sql
SET max_memory_usage = 20000000000;

CREATE TABLE lineorder_flat
ENGINE = MergeTree ORDER BY (LO_ORDERDATE, LO_ORDERKEY)
AS SELECT
    l.LO_ORDERKEY AS LO_ORDERKEY,
    l.LO_LINENUMBER AS LO_LINENUMBER,
    l.LO_CUSTKEY AS LO_CUSTKEY,
    l.LO_PARTKEY AS LO_PARTKEY,
    l.LO_SUPPKEY AS LO_SUPPKEY,
    l.LO_ORDERDATE AS LO_ORDERDATE,
    l.LO_ORDERPRIORITY AS LO_ORDERPRIORITY,
    l.LO_SHIPPRIORITY AS LO_SHIPPRIORITY,
    l.LO_QUANTITY AS LO_QUANTITY,
    l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,
    l.LO_ORDTOTALPRICE AS LO_ORDTOTALPRICE,
    l.LO_DISCOUNT AS LO_DISCOUNT,
    l.LO_REVENUE AS LO_REVENUE,
    l.LO_SUPPLYCOST AS LO_SUPPLYCOST,
    l.LO_TAX AS LO_TAX,
    l.LO_COMMITDATE AS LO_COMMITDATE,
    l.LO_SHIPMODE AS LO_SHIPMODE,
    c.C_NAME AS C_NAME,
    c.C_ADDRESS AS C_ADDRESS,
    c.C_CITY AS C_CITY,
    c.C_NATION AS C_NATION,
    c.C_REGION AS C_REGION,
    c.C_PHONE AS C_PHONE,
    c.C_MKTSEGMENT AS C_MKTSEGMENT,
    s.S_NAME AS S_NAME,
    s.S_ADDRESS AS S_ADDRESS,
    s.S_CITY AS S_CITY,
    s.S_NATION AS S_NATION,
    s.S_REGION AS S_REGION,
    s.S_PHONE AS S_PHONE,
    p.P_NAME AS P_NAME,
    p.P_MFGR AS P_MFGR,
    p.P_CATEGORY AS P_CATEGORY,
    p.P_BRAND AS P_BRAND,
    p.P_COLOR AS P_COLOR,
    p.P_TYPE AS P_TYPE,
    p.P_SIZE AS P_SIZE,
    p.P_CONTAINER AS P_CONTAINER
FROM lineorder AS l
INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY
INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY;
```

Running the queries:

Q1.1

```sql
SELECT
    sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS REVENUE
FROM
    lineorder,
    date
WHERE
    LO_ORDERDATE = D_DATEKEY
    AND D_YEAR = 1993
    AND LO_DISCOUNT BETWEEN 1 AND 3
    AND LO_QUANTITY < 25;
```

Denormalized table:

``` sql
SELECT
    sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
FROM
    lineorder_flat
WHERE
    toYear(LO_ORDERDATE) = 1993
    AND LO_DISCOUNT BETWEEN 1 AND 3
    AND LO_QUANTITY < 25;
```

Q1.2

```sql
SELECT
    sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS REVENUE
FROM
    lineorder,
    date
WHERE
    LO_ORDERDATE = D_DATEKEY
    AND D_YEARMONTHNUM = 199401
    AND LO_DISCOUNT BETWEEN 4 AND 6
    AND LO_QUANTITY BETWEEN 26 AND 35;
```

Denormalized table:

```sql
SELECT
    sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
FROM
    lineorder_flat
WHERE
    toYYYYMM(LO_ORDERDATE) = 199401
    AND LO_DISCOUNT BETWEEN 4 AND 6
    AND LO_QUANTITY BETWEEN 26 AND 35;
```

Q1.3

```sql
SELECT
    sum(LO_EXTENDEDPRICE*LO_DISCOUNT) AS REVENUE
FROM
    lineorder,
    date
WHERE
    LO_ORDERDATE = D_DATEKEY
    AND D_WEEKNUMINYEAR = 6
    AND D_YEAR = 1994
    AND LO_DISCOUNT BETWEEN 5 AND 7
    AND LO_QUANTITY BETWEEN 26 AND 35;
```

Denormalized table:

```sql
SELECT
    sum(LO_EXTENDEDPRICE * LO_DISCOUNT) AS revenue
FROM
    lineorder_flat
WHERE
    toISOWeek(LO_ORDERDATE) = 6
    AND toYear(LO_ORDERDATE) = 1994
    AND LO_DISCOUNT BETWEEN 5 AND 7
    AND LO_QUANTITY BETWEEN 26 AND 35;
```

Q2.1

```sql
SELECT
    sum(LO_REVENUE),
    D_YEAR,
    P_BRAND
FROM
    lineorder,
    date,
    part,
    supplier
WHERE
    LO_ORDERDATE = D_DATEKEY
    AND LO_PARTKEY = P_PARTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND P_CATEGORY = 'MFGR#12'
    AND S_REGION = 'AMERICA'
GROUP BY
    D_YEAR,
    P_BRAND
ORDER BY
    D_YEAR,
    P_BRAND;
```

Denormalized table:

```sql
SELECT
    sum(LO_REVENUE),
    toYear(LO_ORDERDATE) AS year,
    P_BRAND
FROM lineorder_flat
WHERE
    P_CATEGORY = 'MFGR#12'
    AND S_REGION = 'AMERICA'
GROUP BY
    year,
    P_BRAND
ORDER BY
    year,
    P_BRAND;
```

Q2.2

```sql
SELECT
    sum(LO_REVENUE),
    D_YEAR,
    P_BRAND
FROM
    lineorder,
    date,
    part,
    supplier
WHERE
    LO_ORDERDATE = D_DATEKEY
    AND LO_PARTKEY = P_PARTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND P_BRAND BETWEEN
    'MFGR#2221' AND 'MFGR#2228'
    AND S_REGION = 'ASIA'
GROUP BY
    D_YEAR,
    P_BRAND
ORDER BY
    D_YEAR,
    P_BRAND;
```

Denormalized table:

```sql
SELECT
    sum(LO_REVENUE),
    toYear(LO_ORDERDATE) AS year,
    P_BRAND
FROM lineorder_flat
WHERE P_BRAND >= 'MFGR#2221' AND P_BRAND <= 'MFGR#2228' AND S_REGION = 'ASIA'
GROUP BY
    year,
    P_BRAND
ORDER BY
    year,
    P_BRAND;
```

Q2.3

```sql
SELECT
    sum(LO_REVENUE),
    D_YEAR,
    P_BRAND
FROM
    lineorder,
    date,
    part,
    supplier
WHERE
    LO_ORDERDATE = D_DATEKEY
    AND LO_PARTKEY = P_PARTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND P_BRAND = 'MFGR#2221'
    AND S_REGION = 'EUROPE'
GROUP BY
    D_YEAR,
    P_BRAND
ORDER BY
    D_YEAR,
    P_BRAND;
```

Denormalized table:

```sql
SELECT
    sum(LO_REVENUE),
    toYear(LO_ORDERDATE) AS year,
    P_BRAND
FROM lineorder_flat
WHERE P_BRAND = 'MFGR#2239' AND S_REGION = 'EUROPE'
GROUP BY
    year,
    P_BRAND
ORDER BY
    year,
    P_BRAND;
```

Q3.1

```sql
SELECT
    C_NATION,
    S_NATION,
    D_YEAR,
    sum(LO_REVENUE) AS REVENUE
FROM
    customer,
    lineorder,
    supplier,
    date
WHERE
    LO_CUSTKEY = C_CUSTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND LO_ORDERDATE = D_DATEKEY
    AND C_REGION = 'ASIA' AND S_REGION = 'ASIA'
    AND D_YEAR >= 1992 AND D_YEAR <= 1997
GROUP BY
    C_NATION,
    S_NATION,
    D_YEAR
ORDER BY
    D_YEAR ASC,
    REVENUE DESC;
```

Denormalized table:

```sql
SELECT
    C_NATION,
    S_NATION,
    toYear(LO_ORDERDATE) AS year,
    sum(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE
    C_REGION = 'ASIA'
    AND S_REGION = 'ASIA'
    AND year >= 1992
    AND year <= 1997
GROUP BY
    C_NATION,
    S_NATION,
    year
ORDER BY
    year ASC,
    revenue DESC;
```

Q3.2

```sql
SELECT
    C_CITY,
    S_CITY,
    D_YEAR,
    sum(LO_REVENUE) AS REVENUE
FROM
    customer,
    lineorder,
    supplier,
    date
WHERE
    LO_CUSTKEY = C_CUSTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND LO_ORDERDATE = D_DATEKEY
    AND C_NATION = 'UNITED STATES'
    AND S_NATION = 'UNITED STATES'
    AND D_YEAR >= 1992 AND D_YEAR <= 1997
GROUP BY
    C_CITY,
    S_CITY,
    D_YEAR
ORDER BY
    D_YEAR ASC,
    REVENUE DESC;
```

Denormalized table:

```sql
SELECT
    C_CITY,
    S_CITY,
    toYear(LO_ORDERDATE) AS year,
    sum(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE
    C_NATION = 'UNITED STATES'
    AND S_NATION = 'UNITED STATES'
    AND year >= 1992
    AND year <= 1997
GROUP BY
    C_CITY,
    S_CITY,
    year
ORDER BY
    year ASC,
    revenue DESC;
```

Q3.3

```sql
SELECT
    C_CITY,
    S_CITY,
    D_YEAR,
    sum(LO_REVENUE) AS revenue
FROM
    customer,
    lineorder,
    supplier,
    date
WHERE
    LO_CUSTKEY = C_CUSTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND LO_ORDERDATE = D_DATEKEY
    AND (C_CITY = 'UNITED KI1' OR C_CITY = 'UNITED KI5')
    AND (S_CITY = 'UNITED KI1' OR S_CITY = 'UNITED KI5')
    AND D_YEAR >= 1992
    AND D_YEAR <= 1997
GROUP BY
    C_CITY,
    S_CITY,
    D_YEAR
ORDER BY
    D_YEAR ASC,
    revenue DESC;
```

Denormalized table:

```sql
SELECT
    C_CITY,
    S_CITY,
    toYear(LO_ORDERDATE) AS year,
    sum(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE
    (C_CITY = 'UNITED KI1' OR C_CITY = 'UNITED KI5')
    AND (S_CITY = 'UNITED KI1' OR S_CITY = 'UNITED KI5')
    AND year >= 1992
    AND year <= 1997
GROUP BY
    C_CITY,
    S_CITY,
    year
ORDER BY
    year ASC,
    revenue DESC;
```

Q3.4

```sql
SELECT
    C_CITY,
    S_CITY,
    D_YEAR,
    sum(LO_REVENUE) AS revenue
FROM
    customer,
    lineorder,
    supplier,
    date
WHERE
    LO_CUSTKEY = C_CUSTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND LO_ORDERDATE = D_DATEKEY
    AND (C_CITY='UNITED KI1' OR C_CITY='UNITED KI5')
    AND (S_CITY='UNITED KI1' OR S_CITY='UNITED KI5')
    AND D_YEARMONTH = 'Dec1997'
GROUP BY
    C_CITY,
    S_CITY,
    D_YEAR
ORDER BY
    D_YEAR ASC,
    revenue DESC;
```

Denormalized table:

```sql
SELECT
    C_CITY,
    S_CITY,
    toYear(LO_ORDERDATE) AS year,
    sum(LO_REVENUE) AS revenue
FROM lineorder_flat
WHERE
    (C_CITY = 'UNITED KI1' OR C_CITY = 'UNITED KI5')
    AND (S_CITY = 'UNITED KI1' OR S_CITY = 'UNITED KI5')
    AND toYYYYMM(LO_ORDERDATE) = 199712
GROUP BY
    C_CITY,
    S_CITY,
    year
ORDER BY
    year ASC,
    revenue DESC;
```

Q4.1

```sql
SELECT
    D_YEAR,
    C_NATION,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS PROFIT
FROM
    date,
    customer,
    supplier,
    part,
    lineorder
WHERE
    LO_CUSTKEY = C_CUSTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND LO_PARTKEY = P_PARTKEY
    AND LO_ORDERDATE = D_DATEKEY
    AND C_REGION = 'AMERICA'
    AND S_REGION = 'AMERICA'
    AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')
GROUP BY
    D_YEAR,
    C_NATION
ORDER BY
    D_YEAR,
    C_NATION
```

Denormalized table:

```sql
SELECT
    toYear(LO_ORDERDATE) AS year,
    C_NATION,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM lineorder_flat
WHERE C_REGION = 'AMERICA' AND S_REGION = 'AMERICA' AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')
GROUP BY
    year,
    C_NATION
ORDER BY
    year ASC,
    C_NATION ASC;
```

Q4.2

```sql
SELECT
    D_YEAR,
    S_NATION,
    P_CATEGORY,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM
    date,
    customer,
    supplier,
    part,
    lineorder
WHERE
    LO_CUSTKEY = C_CUSTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND LO_PARTKEY = P_PARTKEY
    AND LO_ORDERDATE = D_DATEKEY
    AND C_REGION = 'AMERICA'
    AND S_REGION = 'AMERICA'
    AND (D_YEAR = 1997 OR D_YEAR = 1998)
    AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')
GROUP BY
    D_YEAR,
    S_NATION,
    P_CATEGORY
ORDER BY
    D_YEAR,
    S_NATION,
    P_CATEGORY
```

Denormalized table:

```sql
SELECT
    toYear(LO_ORDERDATE) AS year,
    S_NATION,
    P_CATEGORY,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM lineorder_flat
WHERE
    C_REGION = 'AMERICA'
    AND S_REGION = 'AMERICA'
    AND (year = 1997 OR year = 1998)
    AND (P_MFGR = 'MFGR#1' OR P_MFGR = 'MFGR#2')
GROUP BY
    year,
    S_NATION,
    P_CATEGORY
ORDER BY
    year ASC,
    S_NATION ASC,
    P_CATEGORY ASC;
```

Q4.3

```sql
SELECT
    D_YEAR,
    S_CITY,
    P_BRAND,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM
    date,
    customer,
    supplier,
    part,
    lineorder
WHERE
    LO_CUSTKEY = C_CUSTKEY
    AND LO_SUPPKEY = S_SUPPKEY
    AND LO_PARTKEY = P_PARTKEY
    AND LO_ORDERDATE = D_DATEKEY
    AND C_REGION = 'AMERICA'
    AND S_NATION = 'UNITED STATES'
    AND (D_YEAR = 1997 OR D_YEAR = 1998)
    AND P_CATEGORY = 'MFGR#14'
GROUP BY
    D_YEAR,
    S_CITY,
    P_BRAND
ORDER BY
    D_YEAR,
    S_CITY,
    P_BRAND
```

Denormalized table:

```sql
SELECT
    toYear(LO_ORDERDATE) AS year,
    S_CITY,
    P_BRAND,
    sum(LO_REVENUE - LO_SUPPLYCOST) AS profit
FROM
    lineorder_flat
WHERE
    S_NATION = 'UNITED STATES'
    AND (year = 1997 OR year = 1998)
    AND P_CATEGORY = 'MFGR#14'
GROUP BY
    year,
    S_CITY,
    P_BRAND
ORDER BY
    year ASC,
    S_CITY ASC,
    P_BRAND ASC;
```


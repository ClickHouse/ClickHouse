DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;

CREATE TABLE part
(
    p_partkey       Int32,  -- PK
    p_name          String, -- variable text, size 55
    p_mfgr          FixedString(25),
    p_brand         FixedString(10),
    p_type          String, -- variable text, size 25
    p_size          Int32,  -- integer
    p_container     FixedString(10),
    p_retailprice   Decimal(18,2),
    p_comment       String, -- variable text, size 23
    CONSTRAINT pk CHECK p_partkey >= 0,
    CONSTRAINT positive CHECK (p_size >= 0 AND p_retailprice >= 0)
) engine = MergeTree ORDER BY (p_partkey);

CREATE TABLE supplier
(
    s_suppkey       Int32,  -- PK
    s_name          FixedString(25),
    s_address       String, -- variable text, size 40
    s_nationkey     Int32,  -- FK n_nationkey
    s_phone         FixedString(15),
    s_acctbal       Decimal(18,2),
    s_comment       String, -- variable text, size 101
    CONSTRAINT pk CHECK s_suppkey >= 0
) engine = MergeTree ORDER BY (s_suppkey);

CREATE TABLE partsupp
(
    ps_partkey      Int32,  -- PK(1), FK p_partkey
    ps_suppkey      Int32,  -- PK(2), FK s_suppkey
    ps_availqty     Int32,  -- integer
    ps_supplycost   Decimal(18,2),
    ps_comment      String, -- variable text, size 199
    CONSTRAINT pk CHECK ps_partkey >= 0,
    CONSTRAINT c1 CHECK (ps_availqty >= 0 AND ps_supplycost >= 0)
) engine = MergeTree ORDER BY (ps_partkey, ps_suppkey);

CREATE TABLE customer
(
    c_custkey       Int32,  -- PK
    c_name          String, -- variable text, size 25
    c_address       String, -- variable text, size 40
    c_nationkey     Int32,  -- FK n_nationkey
    c_phone         FixedString(15),
    c_acctbal       Decimal(18,2),
    c_mktsegment    FixedString(10),
    c_comment       String, -- variable text, size 117
    CONSTRAINT pk CHECK c_custkey >= 0
) engine = MergeTree ORDER BY (c_custkey);

CREATE TABLE orders
(
    o_orderkey      Int32,  -- PK
    o_custkey       Int32,  -- FK c_custkey
    o_orderstatus   FixedString(1),
    o_totalprice    Decimal(18,2),
    o_orderdate     Date,
    o_orderpriority FixedString(15),
    o_clerk         FixedString(15),
    o_shippriority  Int32,  -- integer
    o_comment       String, -- variable text, size 79
    CONSTRAINT c1 CHECK o_totalprice >= 0
) engine = MergeTree ORDER BY (o_orderdate, o_orderkey);

CREATE TABLE lineitem
(
    l_orderkey      Int32,  -- PK(1), FK o_orderkey
    l_partkey       Int32,  -- FK ps_partkey
    l_suppkey       Int32,  -- FK ps_suppkey
    l_linenumber    Int32,  -- PK(2)
    l_quantity      Decimal(18,2),
    l_extendedprice Decimal(18,2),
    l_discount      Decimal(18,2),
    l_tax           Decimal(18,2),
    l_returnflag    FixedString(1),
    l_linestatus    FixedString(1),
    l_shipdate      Date,
    l_commitdate    Date,
    l_receiptdate   Date,
    l_shipinstruct  FixedString(25),
    l_shipmode      FixedString(10),
    l_comment       String, -- variable text size 44
    CONSTRAINT c1 CHECK (l_quantity >= 0 AND l_extendedprice >= 0 AND l_tax >= 0 AND l_shipdate <= l_receiptdate)
--  CONSTRAINT c2 CHECK (l_discount >= 0 AND l_discount <= 1)
) engine = MergeTree ORDER BY (l_shipdate, l_receiptdate, l_orderkey, l_linenumber);

CREATE TABLE nation
(
    n_nationkey     Int32,  -- PK
    n_name          FixedString(25),
    n_regionkey     Int32,  -- FK r_regionkey
    n_comment       String, -- variable text, size 152
    CONSTRAINT pk CHECK n_nationkey >= 0
) Engine = MergeTree ORDER BY (n_nationkey);

CREATE TABLE region
(
    r_regionkey     Int32,  -- PK
    r_name          FixedString(25),
    r_comment       String, -- variable text, size 152
    CONSTRAINT pk CHECK r_regionkey >= 0
) engine = MergeTree ORDER BY (r_regionkey);

select 1;
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval 90 day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

select 2, 'fail: correlated subquery'; -- TODO: Missing columns: 'p_partkey'
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and ps_supplycost = (
        select
            min(ps_supplycost)
        from
            partsupp,
            supplier,
            nation,
            region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'EUROPE'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100; -- { serverError 47 }

select 3;
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_mktsegment = 'BUILDING'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
    and l_shipdate > date '1995-03-15'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;

select 4, 'fail: exists'; -- TODO
-- select
--     o_orderpriority,
--     count(*) as order_count
-- from
--     orders
-- where
--     o_orderdate >= date '1993-07-01'
--     and o_orderdate < date '1993-07-01' + interval '3' month
--     and exists (
--         select
--             *
--         from
--             lineitem
--         where
--             l_orderkey = o_orderkey
--             and l_commitdate < l_receiptdate
--     )
-- group by
--     o_orderpriority
-- order by
--     o_orderpriority;

select 5;
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'ASIA'
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
group by
    n_name
order by
    revenue desc;

select 6;
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between toDecimal32(0.06, 2) - toDecimal32(0.01, 2)
        and toDecimal32(0.06, 2) + toDecimal32(0.01, 2)
    and l_quantity < 24;

select 7;
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
            )
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;

select 8;
select
    o_year,
    sum(case
        when nation = 'BRAZIL' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;

select 9;
select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
            s_suppkey = l_suppkey
            and ps_suppkey = l_suppkey
            and ps_partkey = l_partkey
            and p_partkey = l_partkey
            and o_orderkey = l_orderkey
            and s_nationkey = n_nationkey
            and p_name like '%green%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;

select 10;
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '1993-10-01'
    and o_orderdate < date '1993-10-01' + interval '3' month
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;

select 11; -- TODO: remove toDecimal()
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
group by
    ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * toDecimal64('0.0100000000', 2)
            --                                                  ^^^^^^^^^^^^
            -- The above constant needs to be adjusted according
            -- to the scale factor (SF): constant = 0.0001 / SF.
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'GERMANY'
        )
order by
    value desc;

select 12;
select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as low_line_count
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode
order by
    l_shipmode;

select 13, 'fail: join predicates'; -- TODO: Invalid expression for JOIN ON
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc; -- { serverError 403 }

select 14;
select
    toDecimal32(100.00, 2) * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / (1 + sum(l_extendedprice * (1 - l_discount))) as promo_revenue
from
    lineitem,
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month;

select 15, 'fail: correlated subquery'; -- TODO: Missing columns: 'total_revenue'
drop view if exists revenue0;
create view revenue0 as
    select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
    from
        lineitem
    where
        l_shipdate >= date '1996-01-01'
        and l_shipdate < date '1996-01-01' + interval '3' month
    group by
        l_suppkey;
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            revenue0
    )
order by
    s_suppkey; -- { serverError 47 }
drop view revenue0;

select 16;
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    and ps_suppkey not in (
        select
            s_suppkey
        from
            supplier
        where
            s_comment like '%Customer%Complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;

select 17, 'fail: correlated subquery'; -- TODO: Missing columns: 'p_partkey'
select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    ); -- { serverError 47 }

select 18;
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 300
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;

select 19;
select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#34'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );

select 20, 'fail: correlated subquery'; -- TODO: Missing columns: 'ps_suppkey' 'ps_partkey'
select
    s_name,
    s_address
from
    supplier,
    nation
where
    s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
            ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                    p_name like 'forest%'
            )
            and ps_availqty > (
                select
                    0.5 * sum(l_quantity)
                from
                    lineitem
                where
                    l_partkey = ps_partkey
                    and l_suppkey = ps_suppkey
                    and l_shipdate >= date '1994-01-01'
                    and l_shipdate < date '1994-01-01' + interval '1' year
            )
    )
    and s_nationkey = n_nationkey
    and n_name = 'CANADA'
order by
    s_name; -- { serverError 47 }

select 21, 'fail: exists, not exists'; -- TODO
-- select
--     s_name,
--     count(*) as numwait
-- from
--     supplier,
--     lineitem l1,
--     orders,
--     nation
-- where
--     s_suppkey = l1.l_suppkey
--     and o_orderkey = l1.l_orderkey
--     and o_orderstatus = 'F'
--     and l1.l_receiptdate > l1.l_commitdate
--     and exists (
--         select
--             *
--         from
--             lineitem l2
--         where
--             l2.l_orderkey = l1.l_orderkey
--             and l2.l_suppkey <> l1.l_suppkey
--     )
--     and not exists (
--         select
--             *
--         from
--             lineitem l3
--         where
--             l3.l_orderkey = l1.l_orderkey
--             and l3.l_suppkey <> l1.l_suppkey
--             and l3.l_receiptdate > l3.l_commitdate
--     )
--     and s_nationkey = n_nationkey
--     and n_name = 'SAUDI ARABIA'
-- group by
--     s_name
-- order by
--     numwait desc,
--     s_name
-- limit 100;

select 22, 'fail: not exists'; -- TODO
-- select
--     cntrycode,
--     count(*) as numcust,
--     sum(c_acctbal) as totacctbal
-- from
--     (
--         select
--             substring(c_phone from 1 for 2) as cntrycode,
--             c_acctbal
--         from
--             customer
--         where
--             substring(c_phone from 1 for 2) in
--                 ('13', '31', '23', '29', '30', '18', '17')
--             and c_acctbal > (
--                 select
--                     avg(c_acctbal)
--                 from
--                     customer
--                 where
--                     c_acctbal > 0.00
--                     and substring(c_phone from 1 for 2) in
--                         ('13', '31', '23', '29', '30', '18', '17')
--             )
--             and not exists (
--                 select
--                     *
--                 from
--                     orders
--                 where
--                     o_custkey = c_custkey
--             )
--     ) as custsale
-- group by
--     cntrycode
-- order by
--     cntrycode;

DROP TABLE part;
DROP TABLE supplier;
DROP TABLE partsupp;
DROP TABLE customer;
DROP TABLE orders;
DROP TABLE lineitem;
DROP TABLE nation;
DROP TABLE region;

DROP TABLE IF EXISTS adaptive_aggregate_table;
create table adaptive_aggregate_table
(
    ik Int32,
    nik Nullable(Int32),
    sk String,
    nsk Nullable(String),
    low_cardinality_sk LowCardinality(String),
    uuidk UUID,
    ipv4k IPv4,
    ipv6k IPv6
)
Engine=Memory;

insert into adaptive_aggregate_table values (1, 1, 'abc', 'abc', 'abc', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '116.253.40.133', '2001:44c8:129:2632:33:0:252:2'), (2, 3, 'abe', 'abe', 'abe', '61g0c404-5cb3-11e7-907b-a6006ad3dba0', '116.253.40.103', '2001:44c8:129:2632:33:0:252:2'), (1, 1,'abc', 'abc', 'abc', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '116.253.40.133', '2001:44c8:129:2632:33:0:252:2');

set enable_adaptive_aggregation_method = 1;

select ik, sk, count(1) from adaptive_aggregate_table group by ik, sk order by ik, sk;

select sk, nik, count(1) from adaptive_aggregate_table group by sk, nik order by sk, nik;

select sk, nsk, count(1) from adaptive_aggregate_table group by sk, nsk order by sk, nsk;

select sk, low_cardinality_sk, count(1) from adaptive_aggregate_table group by sk, low_cardinality_sk order by sk, low_cardinality_sk;

select sk, uuidk, count(1) from adaptive_aggregate_table group by sk, uuidk order by sk, uuidk;

select sk, ipv4k, count(1) from adaptive_aggregate_table group by sk, ipv4k order by sk, ipv4k;

select sk, ipv6k, count(1) from adaptive_aggregate_table group by sk, ipv6k order by sk, ipv6k;

DROP TABLE IF EXISTS adaptive_aggregate_table;

CREATE TABLE adaptive_aggregate_table(
    `low_card_str_key` String,
    `low_card_int_key` UInt32,
    `high_card_int_key` UInt32
)Engine=Memory;

insert into aggregation_lowcardinality_keys select toString(number%4), number % 4, number from numbers(100);
insert into aggregation_lowcardinality_keys select concat('123456789_', toString(number%100 + 4)), number % 100, number from numbers(200);
insert into aggregation_lowcardinality_keys select toString(number%4), number % 4, number from numbers(100);
select low_card_str_key, low_card_int_key , uniq(high_card_int_key) from default.aggregation_lowcardinality_keys group by low_card_str_key, low_card_int_key order by low_card_str_key, low_card_int_key;

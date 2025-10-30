set enable_partition_distinct=1;

create table if not exists t_partition_distinct
(
    a UInt64,
    b String,
    c String
) engine=Memory;


insert into t_partition_distinct select number % 10, toString(number % 5), 'xxxxxxxxxx' ||toString(number % 5) from numbers(2000) settings max_block_size=512;
-- { echoOn }
select distinct a, b from t_partition_distinct order by a, b;
select distinct a, c from t_partition_distinct order by a, c;
-- { echoOff }

drop table if exists t_partition_distinct;

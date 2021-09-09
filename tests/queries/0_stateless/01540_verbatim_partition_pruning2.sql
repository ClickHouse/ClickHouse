drop table if exists test_partitioning;

create table test_partitioning engine=MergeTree ORDER BY number PARTITION BY cityHash64(number) % 100 as select * from numbers(1000);

-- Trivial count optimization kicks in
set max_rows_to_read = 1;
select count() from test_partitioning where cityHash64(number) % 100 = 1;
select count() from test_partitioning where cityHash64(number) % 100 = 2;
select count() from test_partitioning where cityHash64(number) % 100 IN (1,2);
select count() from test_partitioning where cityHash64(number) % 100 < 2;

drop table test_partitioning;

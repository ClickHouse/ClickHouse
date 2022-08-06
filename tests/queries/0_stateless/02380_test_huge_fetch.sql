-- Tags: long, replica, no-replicated-database

DROP TABLE IF EXISTS fetch_test_huge;
DROP TABLE IF EXISTS fetch_test_huge1;

create table fetch_test_huge (A Int64, huge_column String CODEC(NONE)) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/fetch_test_huge', 'r1') order by A;
insert into fetch_test_huge select number, repeat(' ', 2000) from numbers(1e6) settings max_block_size=8192;
system sync replica fetch_test_huge;
optimize table fetch_test_huge final; 
create table fetch_test_huge1(A Int64, huge_column String CODEC(NONE)) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/fetch_test_huge', 'r2') order by A;
system sync replica fetch_test_huge;
system sync replica fetch_test_huge1;
select count() from fetch_test_huge;
select count() from fetch_test_huge1;
DROP TABLE IF EXISTS fetch_test_huge;
DROP TABLE IF EXISTS fetch_test_huge1;

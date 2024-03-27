-- Tags: no-fasttest
-- Tag: no-fasttest -- requires S3

drop table if exists test_mt;
create table test_mt (a Int32, b Int64) engine = MergeTree() order by a
settings disk = disk(
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/test_mt/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse);

insert into test_mt (*) values (1, 2), (2, 2), (3, 1), (4, 7), (5, 10), (6, 12);
insert into test_mt (*) select number, number from numbers_mt(1000000);

select count(*) from test_mt;
select (*) from test_mt order by tuple(a, b) limit 10;

-- File moving is not supported.
alter table test_mt update b = 0 where a % 2 = 1; --{ serverError NOT_IMPLEMENTED }

alter table test_mt add column c Int64 after b; --{ serverError BAD_GET }
alter table test_mt drop column b; --{ serverError BAD_GET }

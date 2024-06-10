-- Tags: no-fasttest
-- Tag: no-fasttest -- requires S3

drop table if exists test_mt;
create table test_mt (a Int32, b Int64, c Int64) engine = MergeTree() partition by intDiv(a, 1000) order by tuple(a, b)
settings disk = disk(
    name = s3_plain_rewritable,
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/test_mt/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse);

insert into test_mt (*) values (1, 2, 0), (2, 2, 2), (3, 1, 9), (4, 7, 7), (5, 10, 2), (6, 12, 5);
insert into test_mt (*) select number, number, number from numbers_mt(10000);

select count(*) from test_mt;
select (*) from test_mt order by tuple(a, b) limit 10;

optimize table test_mt final;

alter table test_mt add projection test_mt_projection (
    select * order by b); -- { serverError SUPPORT_IS_DISABLED }

alter table test_mt update c = 0 where a % 2 = 1; -- { serverError SUPPORT_IS_DISABLED }
alter table test_mt add column d Int64 after c; -- { serverError SUPPORT_IS_DISABLED }
alter table test_mt drop column c; -- { serverError SUPPORT_IS_DISABLED }

detach table test_mt;
attach table test_mt;

drop table if exists test_mt_dst;

create table test_mt_dst (a Int32, b Int64, c Int64) engine = MergeTree() partition by intDiv(a, 1000) order by tuple(a, b)
settings disk = 's3_plain_rewritable';
alter table test_mt move partition 0 to table test_mt_dst; -- { serverError SUPPORT_IS_DISABLED }

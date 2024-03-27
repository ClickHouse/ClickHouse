-- Tags: no-fasttest
-- Tag: no-fasttest -- requires S3

drop table if exists test_s3;
create table test_s3 (a Int32, b Int64) engine = MergeTree() order by a
settings disk=disk(name='s3_plain_rewritable',
                   type = s3_plain_rewritable,
                   endpoint = 'http://localhost:11111/test/s3_plain_rewritable/',
                   access_key_id = clickhouse,
                   secret_access_key = clickhouse,
                   send_metadata = false, skip_access_check=true
            );

insert into test_s3 (*) values (1, 2), (2, 2), (3, 1), (4, 7), (5, 10), (6, 12);
insert into test_s3 (*) select number, number from numbers_mt(10000000);

select count(*) from test_s3 LIMIT 10;

-- File moving is not supported.
alter table test_s3 update b = 0 where a % 2 = 1; --{ serverError NOT_IMPLEMENTED }

-- TODO: alter table columns.

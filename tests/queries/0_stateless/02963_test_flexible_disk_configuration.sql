-- Tags: no-fasttest

drop table if exists test;
create table test (a Int32) engine = MergeTree() order by tuple()
settings disk=disk(name='test1', type = object_storage, object_storage_type = local_blob_storage, path='./02963_test1/');

drop table test;
create table test (a Int32) engine = MergeTree() order by tuple()
settings disk='s3_disk_02963';

drop table test;
create table test (a Int32) engine = MergeTree() order by tuple()
settings disk=disk(name='test1',
                   type = object_storage,
                   object_storage_type = s3,
                   endpoint = 'http://localhost:11111/test/common/',
                   access_key_id = clickhouse,
                   secret_access_key = clickhouse);

drop table test;
create table test (a Int32) engine = MergeTree() order by tuple()
settings disk=disk(name='test2',
                   type = object_storage,
                   object_storage_type = s3,
                   metadata_storage_type = local,
                   endpoint = 'http://localhost:11111/test/common/',
                   access_key_id = clickhouse,
                   secret_access_key = clickhouse);

drop table test;
create table test (a Int32) engine = MergeTree() order by tuple()
settings disk=disk(name='test3',
                   type = object_storage,
                   object_storage_type = s3,
                   metadata_type = lll,
                   endpoint = 'http://localhost:11111/test/common/',
                   access_key_id = clickhouse,
                   secret_access_key = clickhouse); -- { serverError UNKNOWN_ELEMENT_IN_CONFIG }

create table test (a Int32) engine = MergeTree() order by tuple()
settings disk=disk(name='test4',
                   type = object_storage,
                   object_storage_type = kkk,
                   metadata_type = local,
                   endpoint = 'http://localhost:11111/test/common/',
                   access_key_id = clickhouse,
                   secret_access_key = clickhouse); -- { serverError UNKNOWN_ELEMENT_IN_CONFIG }

create table test (a Int32) engine = MergeTree() order by tuple()
settings disk=disk(name='test5',
                   type = kkk,
                   object_storage_type = s3,
                   metadata_type = local,
                   endpoint = 'http://localhost:11111/test/common/',
                   access_key_id = clickhouse,
                   secret_access_key = clickhouse); -- { serverError UNKNOWN_ELEMENT_IN_CONFIG }

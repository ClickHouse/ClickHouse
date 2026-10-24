-- Tags: no-fasttest

create table a (x Int64) engine URL('https://example.com/', CSV, headers('foo' = 'bar', 'a' = '13'));
show create a;
create table b (x Int64) engine URL('https://example.com/', CSV, headers());
show create b;
create table c (x Int64) engine S3('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'));
show create c;
create table d (x Int64) engine S3('https://example.s3.amazonaws.com/a.csv', NOSIGN, headers('foo' = 'bar'));
show create d;

create view e (x Int64) as select count() from url('https://example.com/', CSV, headers('foo' = 'bar', 'a' = '13'));
show create e;
create view f (x Int64) as select count() from url('https://example.com/', CSV, headers());
show create f;
create view g (x Int64) as select count() from s3('https://example.s3.amazonaws.com/a.csv', CSV, headers('foo' = 'bar'));
show create g;
create view h (x Int64) as select count() from s3('https://example.s3.amazonaws.com/a.csv', headers('foo' = 'bar'));
show create h;

-- A key-value argument next to headers(...) must parse in either written order, and the table must
-- survive the metadata-load path (DETACH/ATTACH), which is what a server restart runs.
create table i (x Int64) engine S3('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), partition_strategy = 'none');
show create i;
detach table i;
attach table i;
show create i;
create table j (x Int64) engine S3('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, partition_strategy = 'none', headers('foo' = 'bar'));
show create j;

-- The same for the other engines sharing the S3 argument parser: the key-value argument is written
-- first, and ATTACH re-reads the stored order, with headers(...) first.
create table k (x Int64) engine GCS('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, partition_strategy = 'none', headers('foo' = 'bar'));
detach table k;
attach table k;
show create k;
create table l (x Int64) engine OSS('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, partition_strategy = 'none', headers('foo' = 'bar'));
detach table l;
attach table l;
show create l;
create table m (x Int64) engine COSN('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, partition_strategy = 'none', headers('foo' = 'bar'));
detach table m;
attach table m;
show create m;
create table n (x Int64) engine Hudi('https://example.s3.amazonaws.com/t/', NOSIGN, Parquet, compression_method = 'none', headers('foo' = 'bar'));
detach table n;
attach table n;
show create n;
create table o (x Int64) engine S3Queue('https://example.s3.amazonaws.com/q/', NOSIGN, CSV, compression_method = 'none', headers('foo' = 'bar')) settings mode = 'unordered', keeper_path = '/clickhouse/{database}/02968_o';
detach table o;
attach table o;
show create o;

-- The table functions sharing the S3 argument parser, in both orders.
set describe_compact_output = 1;
describe table s3('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table s3('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table gcs('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table gcs('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table oss('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table oss('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table cosn('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table cosn('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table icebergS3('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table icebergS3('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table hudi('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table hudi('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table paimonS3('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table paimonS3('https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table s3Cluster('test_shard_localhost', 'https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table s3Cluster('test_shard_localhost', 'https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table icebergS3Cluster('test_shard_localhost', 'https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table icebergS3Cluster('test_shard_localhost', 'https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table hudiCluster('test_shard_localhost', 'https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table hudiCluster('test_shard_localhost', 'https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');
describe table paimonS3Cluster('test_shard_localhost', 'https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, structure = 'x Int64', headers('foo' = 'bar'));
describe table paimonS3Cluster('test_shard_localhost', 'https://example.s3.amazonaws.com/a.csv', NOSIGN, CSV, headers('foo' = 'bar'), structure = 'x Int64');

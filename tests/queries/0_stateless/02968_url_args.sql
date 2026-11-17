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

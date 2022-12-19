-- Tags: no-parallel

DROP TABLE IF EXISTS test;
create table test (
  a Int64,
  b Int64 materialized a
)
engine = MergeTree()
primary key tuple();

insert into test values (1);
SELECT * FROM test;

select * from test into outfile 'test.native.zstd' format Native;
truncate table test;
insert into test from infile 'test.native.zstd';

SELECT * FROM test;

DROP TABLE test;

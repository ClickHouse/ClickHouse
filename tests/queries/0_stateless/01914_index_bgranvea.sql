DROP TABLE IF EXISTS test;
create table test (id UInt64,insid UInt64,insidvalue Nullable(UInt64), index insid_idx (insid) type bloom_filter() granularity 1, index insidvalue_idx (insidvalue) type bloom_filter() granularity 1) ENGINE=MergeTree() ORDER BY (insid,id);

insert into test values(1,1,1),(2,2,2);

select * from test where insid IN (1) OR insidvalue IN (1);
select * from test where insid IN (1) AND insidvalue IN (1);

DROP TABLE test;

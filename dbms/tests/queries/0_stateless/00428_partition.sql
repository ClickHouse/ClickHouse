-- Not found column date in block. There are only columns: x.
create table test.partition_428 (date MATERIALIZED toDate(0), x UInt64, sample_key MATERIALIZED intHash64(x)) ENGINE=MergeTree(date,sample_key,(date,x,sample_key),8192);
insert into test.partition_428 ( x ) VALUES ( now() );
insert into test.partition_428 ( x ) VALUES ( now()+1 );
alter table test.partition_428 detach partition 197001;
alter table test.partition_428 attach partition 197001;
optimize table test.partition_428;
drop table test.partition_428;

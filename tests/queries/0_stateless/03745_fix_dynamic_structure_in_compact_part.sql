drop table if exists test;
create table test (id UInt64, json JSON) engine=CoalescingMergeTree order by id settings min_bytes_for_wide_part='100G', merge_max_block_size=33, index_granularity=800;
insert into test select number, '{}' from numbers(10000);
alter table test update json = '{"a" : 42}' where id > 9000 settings mutations_sync=1;
optimize table test final;
drop table test;

drop table if exists test;
create table test (id UInt64, json JSON) engine=CoalescingMergeTree order by id settings min_bytes_for_wide_part='100G', merge_max_block_size=333, index_granularity=8128;
insert into test select number, '{}' from numbers(100000);
alter table test update json = '{"a" : 42}' where id > 90000 settings mutations_sync=1;
optimize table test final;
drop table test;

-- Second table with no fixed merge_max_block_size and index_granularity for randomization.
drop table if exists test;
create table test (id UInt64, json JSON) engine=CoalescingMergeTree order by id settings min_bytes_for_wide_part='100G';
insert into test select number, '{}' from numbers(100000);
alter table test update json = '{"a" : 42}' where id > 90000 settings mutations_sync=1;
optimize table test final;
drop table test;


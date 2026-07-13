drop table if exists test;
create table test (a UInt32, b UInt32) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=0, ratio_of_defaults_for_sparse_serialization=0.0, max_suspicious_broken_parts=0, max_suspicious_broken_parts_bytes=0;
insert into test select number, number from numbers(10);

detach table test;
attach table test;

alter table test update b = 42 where 1 settings mutations_sync=2;

detach table test;
attach table test;

select * from test;
drop table test;
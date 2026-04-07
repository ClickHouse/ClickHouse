drop table if exists ttl_test_02129;

create table ttl_test_02129(a Int64, b String, d Date)
Engine=MergeTree partition by d order by a
settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, materialize_ttl_recalculate_only = 0;

system stop ttl merges ttl_test_02129;

insert into ttl_test_02129 select number, '', '2021-01-01' from numbers(10);
alter table ttl_test_02129 add column c Int64 settings mutations_sync=2;

insert into ttl_test_02129 select number, '', '2021-01-01', 1 from numbers(10);
alter table  ttl_test_02129 modify TTL (d + INTERVAL 1 MONTH) DELETE WHERE c=1 settings mutations_sync=2;

select * from ttl_test_02129 order by a, b, d, c;
drop table ttl_test_02129;

drop table if exists ttl_test_02129;

select '==========';

create table ttl_test_02129(a Int64, b String, d Date)
Engine=MergeTree partition by d order by a
settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, materialize_ttl_recalculate_only = 1;

system stop ttl merges ttl_test_02129;

insert into ttl_test_02129 select number, '', '2021-01-01' from numbers(10);
alter table ttl_test_02129 add column c Int64 settings mutations_sync=2, alter_sync=2;

insert into ttl_test_02129 select number, '', '2021-01-01', 1 from numbers(10);
alter table ttl_test_02129 modify TTL (d + INTERVAL 1 MONTH) DELETE WHERE c=1 settings mutations_sync=2, alter_sync=2;

select * from ttl_test_02129 order by a, b, d, c;

select '==========';

system start ttl merges ttl_test_02129;

optimize table ttl_test_02129 final;

select * from ttl_test_02129 order by a, b, d, c;
drop table ttl_test_02129;

drop table if exists tp;

create table tp (x Int32, y Int32, projection p (select x, y order by x)) engine = MergeTree order by y settings min_rows_for_compact_part = 2, min_rows_for_wide_part = 4, min_bytes_for_compact_part = 16, min_bytes_for_wide_part = 32;

insert into tp select number, number from numbers(3);
insert into tp select number, number from numbers(5);

check table tp settings check_query_single_value_result=0;

drop table if exists tp;

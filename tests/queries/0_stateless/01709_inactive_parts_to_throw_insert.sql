drop table if exists data_01709;

create table data_01709 (i int) engine MergeTree order by i settings old_parts_lifetime = 10000000000, min_bytes_for_wide_part = 0, inactive_parts_to_throw_insert = 1;

insert into data_01709 values (1);
insert into data_01709 values (2);

optimize table data_01709 final;

insert into data_01709 values (3); -- { serverError TOO_MANY_PARTS }

drop table data_01709;

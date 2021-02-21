drop table if exists x;

create table x (i int) engine MergeTree order by i settings old_parts_lifetime = 10000000000, min_bytes_for_wide_part = 0, inactive_parts_to_throw_insert = 1;

insert into x values (1);
insert into x values (2);

optimize table x final;

insert into x values (3); -- { serverError 252; }

drop table if exists x;

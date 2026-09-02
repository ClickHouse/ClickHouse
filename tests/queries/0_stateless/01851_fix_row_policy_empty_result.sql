drop table if exists tbl;
create table tbl (s String, i int) engine MergeTree order by i;

insert into tbl values ('123', 123);

drop row policy if exists filter on tbl;
create row policy filter on tbl using (s = 'non_existing_domain') to all;

select * from tbl prewhere s = '123' where i = 123;

drop row policy filter on tbl;
drop table tbl;

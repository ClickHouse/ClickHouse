drop table if exists join_tbl;

create table join_tbl (`id` String, `name` String) engine Join(any, left, id);

insert into join_tbl values ('xxx', 'yyy');

select joinGet('join_tbl', 'name', toLowCardinality('xxx'));

drop table if exists join_tbl;

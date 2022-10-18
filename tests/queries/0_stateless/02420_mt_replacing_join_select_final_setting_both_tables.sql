create table if not exists lhs (x String) engine=ReplacingMergeTree() ORDER BY x SETTINGS force_select_final=1;
create table if not exists rhs (x String) engine=ReplacingMergeTree() ORDER BY x SETTINGS force_select_final=1;

insert into lhs values ('abc');
insert into lhs values ('abc');

insert into rhs values ('abc');
insert into rhs values ('abc');

select count() from lhs inner join rhs on lhs.x = rhs.x;

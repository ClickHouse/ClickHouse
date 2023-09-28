drop table if exists histogram;
create table histogram(num Int64) engine=TinyLog;
insert into histogram values(-1);
insert into histogram values(-1);
select histogram(2)(num) from histogram;
drop table if exists histogram;

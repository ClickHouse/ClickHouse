create table histogram(num Int64) engine=Tinylog;
insert into histogram values(-1);
insert into histogram values(-1);
select histogram(2)(num) from histogram;

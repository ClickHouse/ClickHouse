drop temporary table if exists test;
create temporary table test(id int);

select '======Before Truncate======';
insert into test values(0);
select * from test;

select '======After Truncate And Empty======';
truncate temporary table test;
select * from test;

select '======After Truncate And Insert Data======';
insert into test values(0);
select * from test;

drop temporary table test;

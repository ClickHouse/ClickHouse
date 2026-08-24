drop temporary table if exists test_00670;
create temporary table test_00670(id int);

select '======Before Truncate======';
insert into test_00670 values(0);
select * from test_00670;

select '======After Truncate And Empty======';
truncate temporary table test_00670;
select * from test_00670;

select '======After Truncate And Insert Data======';
insert into test_00670 values(0);
select * from test_00670;

drop temporary table test_00670;

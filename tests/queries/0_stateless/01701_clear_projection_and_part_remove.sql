drop table if exists tp_1;
-- In this test, we are going to create an old part with written projection which does not exist in table metadata
create table tp_1 (x Int32, y Int32, projection p (select x, y order by x)) engine = MergeTree order by y partition by intDiv(y, 100) settings old_parts_lifetime=1;
insert into tp_1 select number, number from numbers(3);
set mutations_sync = 2;
alter table tp_1 add projection pp (select x, count() group by x);
insert into tp_1 select number, number from numbers(4);
-- Here we have a part with written projection pp
alter table tp_1 detach partition '0';
-- Move part to detached
alter table tp_1 clear projection pp;
-- Remove projection from table metadata
alter table tp_1 drop projection pp;
-- Now, we don't load projection pp for attached part, but it is written on disk
alter table tp_1 attach partition '0';
-- Make this part obsolete
optimize table tp_1 final;
-- Now, DROP TABLE triggers part removal
drop table tp_1;

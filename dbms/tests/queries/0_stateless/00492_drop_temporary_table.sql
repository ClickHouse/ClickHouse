drop table if exists temp_tab;
create temporary table temp_tab (number UInt64);
insert into temp_tab select number from system.numbers limit 1;
select number from temp_tab;
drop table temp_tab;
create temporary table temp_tab (number UInt64);
select number from temp_tab;
drop table temp_tab;

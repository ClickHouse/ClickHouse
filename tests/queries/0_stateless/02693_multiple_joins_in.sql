create temporary table temp_table3(val0 UInt64) ENGINE=Memory();
select * from (select 1 as id) t1 inner join (select 1 as id) t2 on t1.id=t2.id inner join (select 1 as id) t3 on t1.id=t3.id where t1.id in temp_table3;
select * from (select 1 as id) t1 inner join (select 1 as id) t2 on t1.id=t2.id where t1.id in temp_table3;

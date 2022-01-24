select t1.dummy, t2.dummy, t3.dummy from system.one t1 join system.one t2 on t1.dummy = t2.dummy join system.one t3 ON t1.dummy = t3.dummy;
select * from system.one t1 join system.one t2 on t1.dummy = t2.dummy join system.one t3 ON t1.dummy = t3.dummy;
select t1.* from system.one t1 join system.one t2 on t1.dummy = t2.dummy join system.one t3 ON t1.dummy = t3.dummy;
select t2.*, t3.* from system.one t1 join system.one t2 on t1.dummy = t2.dummy join system.one t3 ON t1.dummy = t3.dummy;
select t1.dummy, t2.*, t3.dummy from system.one t1 join system.one t2 on t1.dummy = t2.dummy join system.one t3 ON t1.dummy = t3.dummy;
select t1.dummy, t2.*, t3.dummy from system.one t1 join (select dummy,0 as another_one from system.one) t2 on t1.dummy = t2.dummy OR t1.dummy = t2.another_one join system.one t3 ON t1.dummy = t3.dummy;

select t1.dummy, t2.*, t3.dummy from (select * from system.one) t1
join system.one t2 on t1.dummy = t2.dummy
join system.one t3 ON t1.dummy = t3.dummy;

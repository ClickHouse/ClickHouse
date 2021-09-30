create table node (id UInt32, name String) engine = MergeTree() order by id;
insert into node values (0,'wang'),(1,'zhang'),(2,'liu'),(3, 'ding'),(4, 'sun'),(5,'wang'),(6,'zhang'),(7,'liu'),(8, 'ding'),(9, 'sun'),(10, 'sun');

select * from node limit -5;
select * from node limit -5 offset -2;
select * from node order by name limit -5;
select * from node order by name limit -5 offset -2;
select * from node order by name limit -5 with ties;
select * from (select * from node order by name) order by name limit -5;
select * from numbers(10) limit -3;
DROP TABLE IF EXISTS c;
create view c as select 3 as result where {a:Int16} = 0;

SET enable_analyzer = 1;
select 1, result from c(a=0);
select 2, result from c(a=3);

DROP TABLE c;

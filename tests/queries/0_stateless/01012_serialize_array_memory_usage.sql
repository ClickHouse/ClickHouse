-- Tags: no-replicated-database

-- serialization of big arrays shouldn't use too much memory
set max_memory_usage = 300000000;
select ignore(x) from (select groupArray(number) x from numbers(3355443)) group by x format Null;

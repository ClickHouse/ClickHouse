-- serialization of big arrays shouldn't use too much memory
set max_memory_usage = 3000000000;
select ignore(x) from (select groupArray(number) x from numbers(33554433)) group by x format Null;

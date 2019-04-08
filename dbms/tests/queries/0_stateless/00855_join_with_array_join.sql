set allow_experimental_multiple_joins_emulation = 0;
set allow_experimental_cross_to_join_conversion = 0;
select ax, c from (select [1,2] ax, 0 c) array join ax join (select 0 c) using(c);
select ax, c from (select [3,4] ax, 0 c) join (select 0 c) using(c) array join ax;
select ax, c from (select [5,6] ax, 0 c) s1 join system.one s2 ON s1.c = s2.dummy array join ax;

set allow_experimental_multiple_joins_emulation = 1;
set allow_experimental_cross_to_join_conversion = 1;
select ax, c from (select [1,2] ax, 0 c) array join ax join (select 0 c) using(c);
select ax, c from (select [3,4] ax, 0 c) join (select 0 c) using(c) array join ax;
select ax, c from (select [5,6] ax, 0 c) s1 join system.one s2 ON s1.c = s2.dummy array join ax;

select ax, c from (select [7,8] ax, 0 c) s1
join system.one s2 ON s1.c = s2.dummy
join system.one s3 ON s1.c = s3.dummy
array join ax; -- { serverError 48 }

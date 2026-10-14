with l_t as (select 42 as key) select * from  l_t inner join numbers(50) as r_t on l_t.key = r_t.number and r_t.number in (select number * 2 from numbers(1e3)) SETTINGS enable_analyzer=0;
with l_t as (select 42 as key) select * from  l_t inner join numbers(50) as r_t on l_t.key = r_t.number and r_t.number in (select number * 2 from numbers(1e3)) SETTINGS enable_analyzer=1;

with l_t as (select 42 as key) select * from  l_t inner join numbers(50) as r_t on l_t.key = r_t.number and r_t.number global in (select number * 2 from numbers(1e3)) SETTINGS enable_analyzer=1;
with l_t as (select 42 as key) select * from  l_t inner join numbers(50) as r_t on l_t.key = r_t.number and r_t.number global in (select number * 2 from numbers(1e3)) SETTINGS enable_analyzer=0;

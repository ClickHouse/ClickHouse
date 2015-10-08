select b from (select 1 as a, 42 as c) any left join (select 2 as b, 2 as b, 41 as c) using c;
select b from (select 1 as a, 42 as c) any left join (select 2 as b, 2 as b, 42 as c) using c;

select c,a,a,b,b from
  (select 1 as a, 1 as a, 42 as c group by c order by a,c)
 any left join
  (select 2 as b, 2 as b, 41 as c group by c order by b,c)
 using c
 order by b;

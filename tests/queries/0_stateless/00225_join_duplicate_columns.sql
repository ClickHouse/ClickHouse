select b from (select 1 as a, 42 as c) js1 any left join (select 2 as b, 2 as b, 41 as c) js2 using c;
select b from (select 1 as a, 42 as c) js1 any left join (select 2 as b, 2 as b, 42 as c) js2 using c;

select c,a,a,b,b from
  (select 1 as a, 1 as a, 42 as c group by c order by a,c) js1
 any left join
  (select 2 as b, 2 as b, 41 as c group by c order by b,c) js2
 using c
 order by b;

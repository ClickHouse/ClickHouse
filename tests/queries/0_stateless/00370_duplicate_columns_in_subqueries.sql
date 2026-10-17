SET any_join_distinct_right_table_keys = 1;
SET joined_subquery_requires_alias = 0;

select x, y from (select 1 as x, 2 as y, x, y);
select x, y from (select 1 as x, 1 as y, x, y);
select x from (select 1 as x, 1 as y, x, y);
select * from (select 1 as x, 2 as y, x, y);
select * from (select 1 as a, 1 as b, 1 as c, b, c);
select b, c from (select 1 as a, 1 as b, 1 as c, b, c);
select b, c from (select 1 as a, 1 as b, 1 as c, b, c) any left join (select 1 as a) using a;
select b, c from (select 1 as a, 1 as b, 1 as c, 1 as b, 1 as c) any left join (select 1 as a) using a;
select a, b, c from (select 42 as a, 1 as b, 2 as c, 1 as b, 2 as c) any left join (select 42 as a, 3 as d) using a;
select a, b, c from (select 42 as a, 1 as b, 2 as c, 1 as b, 2 as c) any left join (select 42 as a, 3 as d) using a order by d;

SELECT k, a1, b1, a2, b2 FROM (SELECT 0 AS k, 'hello' AS a1, 'world' AS b1, a1) ANY FULL OUTER JOIN (SELECT 1 AS k, 'hello' AS a2, 'world' AS b2, a2) USING (k) ORDER BY k;

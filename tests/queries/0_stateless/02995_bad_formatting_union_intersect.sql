create temporary table t1 engine=MergeTree() order by c as ( select 1 as c intersect (select 1 as c union all  select 2 as c ) );
SELECT * FROM t1;

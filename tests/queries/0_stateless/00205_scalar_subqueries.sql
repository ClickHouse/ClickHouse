SET send_logs_level = 'fatal';

SELECT (SELECT (SELECT (SELECT (SELECT (SELECT count() FROM (SELECT * FROM system.numbers LIMIT 10)))))) = (SELECT 10), ((SELECT 1, 'Hello', [1, 2]).3)[1];
SELECT toUInt64((SELECT 9)) IN (SELECT number FROM system.numbers LIMIT 10);
SELECT (SELECT toDate('2015-01-02')) = toDate('2015-01-02'), 'Hello' = (SELECT 'Hello');
SELECT (SELECT toDate('2015-01-02'), 'Hello');
SELECT (SELECT toDate('2015-01-02'), 'Hello') AS x, x, identity((SELECT 1)), identity((SELECT 1) AS y);
-- SELECT (SELECT uniqState(''));

 SELECT ( SELECT throwIf(1 + dummy) );  -- { serverError 395 }

-- Scalar subquery with 0 rows must return Null
SELECT (SELECT 1 WHERE 0);
-- But tuple and array can't be inside nullable
SELECT (SELECT 1, 2 WHERE 0); -- { serverError 125 }
SELECT (SELECT [1] WHERE 0); -- { serverError 125 }
-- Works for not-empty casle
SELECT (SELECT 1, 2);
SELECT (SELECT [1]);
-- Several rows
SELECT (SELECT number FROM numbers(2)); -- { serverError 125 }

-- Bug reproduction form #25411
WITH a AS (select (select 1 WHERE 0) as b)
select 1
from system.one
cross join a
where a.b = 0;

-- Reported query
drop table if exists t_q1ht4gq_5;
create table t_q1ht4gq_5 (c_zeij INTEGER NOT NULL, c_fehk75l TEXT, c_jz TEXT, c_wynzuek TEXT, c_nkt INTEGER NOT NULL, c_g TEXT, c_mc2 TEXT, primary key(c_nkt)) engine = MergeTree();
WITH
cte_0 AS (select
    subq_0.c6 as c2,
    case when 0<>0 then ((select c_zeij from t_q1ht4gq_5 order by c_zeij limit 1 offset 1)
           + subq_0.c4) else ((select c_zeij from t_q1ht4gq_5 order by c_zeij limit 1 offset 1)
           + subq_0.c4) end as c4 
  from
    (select  
          ref_0.c_nkt as c4, 
          ref_0.c_nkt as c6  
        from 
          t_q1ht4gq_5 as ref_0
        ) as subq_0
  )
select
    ref_12.c_zeij as c3
  from
    t_q1ht4gq_5 as ref_12
  where (ref_12.c_jz not in (
          select
              ref_14.c_mc2 as c0
            from
              t_q1ht4gq_5 as ref_14
                cross join cte_0 as ref_15
            where ref_15.c4 > ref_15.c2));

drop table if exists t_q1ht4gq_5;

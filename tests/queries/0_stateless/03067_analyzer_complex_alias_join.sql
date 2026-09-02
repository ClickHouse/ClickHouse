SET enable_analyzer=1;
with d as (select 'key'::Varchar(255) c, 'x'::Varchar(255) s)
SELECT r1, c as r2
FROM (
       SELECT t as s, c as r1
       FROM ( SELECT 'y'::Varchar(255) as t, 'x'::Varchar(255) as s) t1
       LEFT JOIN d USING (s)
     ) t2
LEFT JOIN d using (s)
SETTINGS join_use_nulls=1;

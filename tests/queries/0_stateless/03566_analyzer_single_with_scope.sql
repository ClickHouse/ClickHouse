create table tab (x String, y UInt8) engine = MergeTree order by tuple();
insert into tab select 'rue', 1;

with ('t' || x) as y 
  select 1 from tab where y = 'true' settings enable_analyzer=0;
with ('t' || x) as y 
  select 1 from tab where y = 'true' settings enable_analyzer=1;

with ('t' || x) as y select * from 
  (select 1 from tab where y = 'true') settings enable_analyzer=0;
with ('t' || x) as y select * from 
  (select 1 from tab where y = 'true') settings enable_analyzer=1; -- { serverError TYPE_MISMATCH }

with
  ('t' || x) as y,
  'rue' as x
select 1 from tab where y = 'true' settings enable_analyzer=1;

with ('t' || x) as y,
  'rue' as x 
select * from 
  (select 1 from tab where y = 'true') settings enable_analyzer=1; -- { serverError TYPE_MISMATCH }

SET enable_scopes_for_with_statement = 0;

SELECT 'ENABLED COMPATIBILITY MODE';

with ('t' || x) as y select * from 
  (select 1 from tab where y = 'true') settings enable_analyzer=1;

with
  ('t' || x) as y,
  'rue' as x
select 1 from tab where y = 'true' settings enable_analyzer=1;

with ('t' || x) as y,
  'rue' as x 
select * from 
  (select 1 from tab where y = 'true') settings enable_analyzer=1;

with
  ('t' || x) as y,
  'rue' as x
select 1 from tab where y = 'true' settings enable_analyzer=0; -- { serverError TYPE_MISMATCH }

with ('t' || x) as y,
  'rue' as x 
select * from 
  (select 1 from tab where y = 'true') settings enable_analyzer=0; -- { serverError TYPE_MISMATCH }

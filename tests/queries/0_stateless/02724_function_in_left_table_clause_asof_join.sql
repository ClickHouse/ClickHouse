select count(*)
from (
  select 1 as id, [1, 2, 3] as arr
) as sessions
ASOF LEFT JOIN (
  select 1 as session_id, 4 as id
) as visitors
ON visitors.session_id <= sessions.id AND arrayFirst(a -> a, arrayMap((a) -> a, sessions.arr)) = visitors.id
;

select count(*)
from (
  select 1 as id, [1, 2, 3] as arr
) as sessions
ASOF LEFT JOIN (
  select 1 as session_id, 4 as id
) as visitors
ON visitors.session_id <= sessions.id AND arrayFirst(a -> a, arrayMap((a) -> a, sessions.arr)) = visitors.id
SETTINGS join_algorithm = 'full_sorting_merge'
;

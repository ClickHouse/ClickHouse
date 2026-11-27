SET enable_analyzer = 1;

select *
from (select 1 a) t
left join (select 1 b where false) u on true;

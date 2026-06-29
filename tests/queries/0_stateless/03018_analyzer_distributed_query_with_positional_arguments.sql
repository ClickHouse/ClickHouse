select 0 as x
from remote('127.0.0.{1,2}', system.one)
group by x;

select 0 as x
from remote('127.0.0.{1,2}', system.one)
order by x;

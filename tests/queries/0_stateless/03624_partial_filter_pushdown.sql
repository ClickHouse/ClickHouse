select * from (
    explain actions=1
    select n1.number, n2.number
    from numbers(5) as n1, numbers(6) as n2
    where (n1.number = 1 and n2.number = 2) or (n1.number = 3 and n2.number = 4)
) where explain like '%Filter%' or explain like '%Join%' or explain like '%Read%';

select n1.number, n2.number
from numbers(5) as n1, numbers(6) as n2
where (n1.number = 1 and n2.number = 2) or (n1.number = 3 and n2.number = 4);

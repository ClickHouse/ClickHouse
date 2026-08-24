select a
from (
    with a+1 as aa,
        sumIf(aa, b > 0) as aaif
    select a, aaif
    FROM (select 1 as a, 2 as b)
    GROUP BY a
) as V;

select a
from (
    with a+1 as aa
        -- , sumIf(c, b > 0) as aaif
        , sum(if(b>0,c,0))  as aaif2
    select a, aaif2
    FROM
        (select 1 as a, 2 as b, 3 as c)
    GROUP BY a
) as V;

select a
from (
    with a+1 as aa
        -- , sumIf(c, b > 0) as aaif
        -- , sum(if(b>0,c,0))  as aaif2
    select a, sumIf(c, b > 0) as aaif3
    FROM
        (select 1 as a, 2 as b, 3 as c)
    GROUP BY a
) as V;

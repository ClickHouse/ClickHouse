SET join_use_nulls = 1;

select c FROM (
    select
        d2.c
    from ( select 1 as a, 2 as b ) d1
    FULL join ( select 1 as a, 3 as c ) d2
        on (d1.a = d2.a)
)
;

with d1 as (
    select
        1 as a,
        2 as b
),
d2 as (
    select
        1 as a,
        3 as c
),
joined as (
    select
        d1.*,
        d2.c
    from d1
    inner join d2
        on (d1.a = d2.a)
)
select c
from joined;

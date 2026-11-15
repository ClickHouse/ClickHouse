SET join_use_nulls = 1;

SELECT '--';

select c FROM (
    select
        d2.c
    from ( select 1 as a, 2 as b ) d1
    FULL join ( select 1 as a, 3 as c ) d2
        on (d1.a = d2.a)
)
;

SELECT '--';

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

SELECT '--';

WITH
    a AS ( SELECT 0 AS key, 'a' AS acol ),
    b AS ( SELECT 2 AS key )
SELECT a.key
FROM b
LEFT JOIN a ON 1
LEFT JOIN a AS a1 ON 1
;

SELECT '--';

WITH
    a AS ( SELECT 0 AS key, 'a' AS acol ),
    b AS ( SELECT 2 AS key )
SELECT a.acol, a1.acol
FROM b
LEFT JOIN a ON a.key = b.key
LEFT JOIN a AS a1 ON a1.key = a.key
;
SELECT '--';

WITH
    a AS ( SELECT 0 AS key, 'a' AS acol ),
    b AS ( SELECT 2 AS key )
SELECT a.acol, a1.acol
FROM b
FULL JOIN a ON a.key = b.key
FULL JOIN a AS a1 ON a1.key = a.key
ORDER BY 1
SETTINGS join_use_nulls = 0
;

SELECT '--';

WITH
    a AS ( SELECT 0 AS key, 'a' AS acol ),
    b AS ( SELECT 2 AS key )
SELECT a.acol, a1.acol
FROM b
FULL JOIN a ON a.key = b.key
FULL JOIN a AS a1 ON a1.key = a.key
ORDER BY 1
;

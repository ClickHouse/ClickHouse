SET joined_subquery_requires_alias = 0;

select toTypeName(key), toTypeName(value) from (
    select 1 as key, '' as value
    union all
    select toUInt64(2) as key, toNullable('') as value
);

select toTypeName(key), toTypeName(value) from (
    select toDecimal64(2, 8) as key, toNullable('') as value
    union all
    select toDecimal32(2, 4) as key, toFixedString('', 1) as value
);

select * from (
    select 'v1' as c1, null as c2
    union all
    select 'v2' as c1, '' as c2
) ALL FULL JOIN (
    select 'v1' as c1, 'w1' as c2
) using c1,c2
order by c1, c2;

select key, s1.value, s2.value
from (
    select 'key1' as key, 'value1' as value
) s1
all left join (
    select 'key1' as key, '' as value
    union all
    select 'key2' as key, toNullable('') as value
) s2
using key;

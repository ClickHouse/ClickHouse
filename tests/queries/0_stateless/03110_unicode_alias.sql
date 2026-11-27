-- https://github.com/ClickHouse/ClickHouse/issues/47288

SET enable_analyzer=1;

select 1 as `c0`
from (
        select C.`字段` AS `字段`
        from (
                select 2 as bb
            ) A
            LEFT JOIN (
                select '1' as `字段`
            ) C ON 1 = 1
            LEFT JOIN (
                select 1 as a
            ) D ON 1 = 1
    ) as `T0`
where `T0`.`字段` = '1';

select 1 as `c0`
from (
        select C.`＄` AS `＄`
        from (
                select 2 as bb
            ) A
            LEFT JOIN (
                select '1' as `＄`
            ) C ON 1 = 1
            LEFT JOIN (
                select 1 as a
            ) D ON 1 = 1
    ) as `T0`
where `T0`.`＄` = '1';

select `字段a`, `字段b`
from (
        select 2 as `字段a`
    ) A
    LEFT JOIN (
        select '1' as `字段b`
    ) B ON 1 = 1;

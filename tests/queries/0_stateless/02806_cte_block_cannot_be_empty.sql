with c as ( select 1 ID, toDate('2023-06-24') dt, 0 p ) select multiIf(t.ID = 1, formatRowNoNewline('JSONEachRow', dd), '') AS params     from (select ID, case when p = 0 then toString(date_add(hour, p, dt)) else '2022-01-01' end as dd from c) t;
with c as ( select 1 ID, toDate('2023-06-24') dt, 0 p ) select multiIf(t.ID = 1, formatRowNoNewline('JSONEachRow', dd), '') AS params, dd from (select ID, case when p = 0 then toString(date_add(hour, p, dt)) else '2022-01-01' end as dd from c) t;

select
    if(
        outer_table.condition_value = 1,
        formatRowNoNewline('JSONEachRow', outer_table.result_date),
        ''
    ) as json
from (
        select
            1 as condition_value,
            date_add(month, inner_table.offset, toDate('2023-06-24')) as result_date
        from (
            select
                2 as offset
            ) inner_table
    ) outer_table;

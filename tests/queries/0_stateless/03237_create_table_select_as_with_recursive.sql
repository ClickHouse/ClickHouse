drop table if exists t;
create table t1 (a Int64, s DateTime('Asia/Istanbul')) Engine = MergeTree() ORDER BY a;
create view t AS (
    WITH RECURSIVE 42 as ttt,
    toDate(s) as start_date,
    _table as (select 1 as number union all select number + 1 from _table where number < 10)
    SELECT a, ttt from t1 join _table on t1.a = _table.number and start_date = '2024-09-23'
);
drop table t;

-- Tags: no-random-settings

create table if not exists t_group_by_lowcardinality(p_date Date, val LowCardinality(Nullable(String)))
engine=MergeTree() partition by p_date order by tuple();

insert into t_group_by_lowcardinality select today() as p_date, toString(number/5) as val from numbers(10000);
insert into t_group_by_lowcardinality select today() as p_date, Null as val from numbers(100);

-- Regression test for: GROUP BY LowCardinality(Nullable(String)) with group_by_overflow_mode='any'
-- must not crash. Originally introduced in PR #29637 / #56057.
--
-- The exact set of returned rows is non-deterministic: with overflow mode 'any', only the first
-- `max_rows_to_group_by` distinct keys end up in the hash table, and which keys those are depends
-- on parallel execution / parallel replicas / S3 read ordering. `LIMIT 10` without `ORDER BY`
-- further selects 10 of those keys non-deterministically. Therefore we only assert that the query
-- runs successfully and returns exactly the LIMIT number of rows.
select count() from (
    select val, avg(toUInt32(val)) from t_group_by_lowcardinality group by val limit 10 settings max_threads=1, max_rows_to_group_by=100, group_by_overflow_mode='any'
) format JSONEachRow;

drop table if exists t_group_by_lowcardinality;

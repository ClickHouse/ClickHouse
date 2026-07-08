-- Previously, the logs looked like this:
--
-- (SelectExecutor): Key condition: unknown
-- (SelectExecutor): Query condition cache has dropped 3/25 granules for PREWHERE condition notEquals(__table1.s, 'xxx'_String).
-- (SelectExecutor): Query condition cache has dropped 0/22 granules for WHERE condition notEquals(s, 'xxx'_String).
-- (SelectExecutor): Filtering marks by primary and secondary keys
-- (SelectExecutor): PK index has dropped 3/25 granules, it took 0ms across 1 threads.
-- (SelectExecutor): Selected 1/1 parts by partition key, 1 parts by primary key, 22/25 marks by primary key, 22 marks to read from 1 ranges
-- (SelectExecutor): Spreading mark ranges among streams (default reading)
-- (SelectExecutor): Reading approx. 180224 rows with 2 streams
--
-- I.e., we reported that PK analysis processed all the granules, while some of them were already dropped by the QCC. Looks confusing.


drop table if exists t;

create table t(a UInt64, s String) engine=MergeTree order by a settings index_granularity=8192, index_granularity_bytes=0, min_rows_for_wide_part=0, min_bytes_for_wide_part=0;

insert into t select number, toString(number) from numbers_mt(1e5);
insert into t select number + 1e6, 'xxx' from numbers_mt(1e5);
optimize table t final;

set enable_analyzer=1;
set use_query_condition_cache=1;

-- Pre-warm the query condition cache
select avg(a) from t where s != 'xxx' format Null;

select avg(a) from t where s != 'xxx' settings log_comment = '04051_pk_analysis_stats_query' format Null;

system flush logs query_log;

WITH (
    SELECT sum(marks) AS total_marks
    FROM system.parts
    WHERE table = 't' AND database = currentDatabase() AND active
) AS total_marks
SELECT throwIf(ProfileEvents['FilteringMarksWithPrimaryKeyProcessedMarks'] >= total_marks)
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '04051_pk_analysis_stats_query') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT Null;

drop table t;

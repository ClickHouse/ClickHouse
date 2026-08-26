
SET max_ast_depth = 10_000_000;

SELECT
    val,
    val + 1 as prev,
    val + prev as val
FROM ( SELECT 1 as val )
; -- { serverError CYCLIC_ALIASES, UNKNOWN_IDENTIFIER, TOO_DEEP_RECURSION }


SELECT
    val,
    val + 1 as prev,
    val + prev as val2
FROM ( SELECT 1 as val )
;

select number % 2 as number, count() from numbers(10) where number != 0 group by number % 2 as number;

CREATE TABLE test_table (time_stamp_utc DateTime, impressions UInt32, clicks UInt32, revenue Float32) ENGINE = MergeTree ORDER BY time_stamp_utc;

SELECT
    toStartOfDay(toDateTime(time_stamp_utc)) AS time_stamp_utc,
    sum(impressions) AS Impressions,
    sum(clicks) AS Clicks,
    sum(revenue) AS Revenue
FROM test_table
WHERE (time_stamp_utc >= toDateTime('2024-04-25 00:00:00')) AND (time_stamp_utc < toDateTime('2024-05-02 00:00:00'))
GROUP BY time_stamp_utc
ORDER BY Impressions DESC
LIMIT 1000;

drop table test_table;
create table test_table engine MergeTree order by sum as select 100 as sum union all select 200 as sum;
select sum as sum from (select sum(sum) as sum from test_table);

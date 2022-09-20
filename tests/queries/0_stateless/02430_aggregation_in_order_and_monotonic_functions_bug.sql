drop table if exists x3;
CREATE TABLE x3(t DateTime, c1 String) ENGINE = MergeTree ORDER BY (t, c1)
as select '2022-09-09 12:00:00', toString(number%2) from numbers(2) union all
select '2022-09-09 12:00:30', toString(number%2)||'x' from numbers(3);

select count() from
(SELECT toStartOfMinute(t) AS s, c1 FROM x3 GROUP BY s, c1 limit 100000000)
settings optimize_aggregation_in_order=1;


SELECT avg(number) AS number, max(number) FROM numbers(10); -- { serverError ILLEGAL_AGGREGATION }
SELECT sum(x) AS x, max(x) FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) t; -- { serverError ILLEGAL_AGGREGATION }
select sum(C1) as C1, count(C1) as C2 from (select number as C1 from numbers(3)) as ITBL; -- { serverError ILLEGAL_AGGREGATION }

set prefer_column_name_to_alias  = 1;
SELECT avg(number) AS number, max(number) FROM numbers(10);
SELECT sum(x) AS x, max(x) FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) t settings prefer_column_name_to_alias = 1;
select sum(C1) as C1, count(C1) as C2 from (select number as C1 from numbers(3)) as ITBL settings prefer_column_name_to_alias = 1;

DROP TABLE IF EXISTS mytable;
CREATE TABLE IF NOT EXISTS mytable (start_ts UInt32, end_ts UInt32, uuid String) ENGINE = MergeTree() ORDER BY start_ts;
INSERT INTO mytable VALUES (1, 2, 3);

SELECT any(uuid) AS id, max(end_ts) - any(start_ts) AS time_delta, any(start_ts) AS start_ts, max(end_ts) AS end_ts FROM mytable GROUP BY uuid HAVING max(end_ts) < 1620141001 ORDER BY any(start_ts) DESC;

SELECT any(uuid) AS id, max(end_ts) - any(start_ts) AS time_delta, any(start_ts) AS start_ts, max(end_ts) AS end_ts FROM mytable GROUP BY uuid HAVING max(end_ts) < 1620141001 ORDER BY any(start_ts) DESC SETTINGS prefer_column_name_to_alias=1;

DROP TABLE mytable;

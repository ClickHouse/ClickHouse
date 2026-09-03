-- https://github.com/ClickHouse/ClickHouse/issues/29748
SET enable_analyzer=1;

create table events ( distinct_id String ) engine = Memory;

INSERT INTO events VALUES ('1234'), ('1');

WITH cte1 as (
    SELECT '1234' as x
 ), cte2 as (
    SELECT '1234' as x
   )
SELECT *
FROM events AS events
JOIN cte2 ON cte2.x = events.distinct_id
JOIN cte1 ON cte1.x = cte2.x
limit 1;

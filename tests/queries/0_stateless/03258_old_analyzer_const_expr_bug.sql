WITH
  multiIf('-1' = '-1', 10080, '-1' = '7', 60, '-1' = '1', 5, 1440) AS interval_start, -- noqa
  multiIf('-1' = '-1', CEIL((today() - toDate('2017-06-22')) / 7)::UInt16, '-1' = '7', 168, '-1' = '1', 288, 90) AS days_run, -- noqa:L045
  block_time as (SELECT arrayJoin(
        arrayMap(
            i -> toDateTime(toStartOfInterval(now(), INTERVAL interval_start MINUTE) - interval_start * 60 * i, 'UTC'),
            range(days_run)
        )
    )),

sales AS (
    SELECT
        toDateTime(toStartOfInterval(now(), INTERVAL interval_start MINUTE), 'UTC') AS block_time
    FROM
        numbers(1)
    GROUP BY
        block_time
    ORDER BY
        block_time)

SELECT
    block_time
FROM sales where block_time >= (SELECT MIN(block_time) FROM sales) format Null;

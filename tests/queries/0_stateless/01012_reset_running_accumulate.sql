-- Disable external aggregation because the state is reset for each new block of data in 'runningAccumulate' function.
SET max_bytes_before_external_group_by = 0;

SELECT grouping,
       item,
       runningAccumulate(state, grouping)
FROM (
      SELECT number % 6 AS grouping,
             number AS item,
             sumState(number) AS state
      FROM (SELECT number FROM system.numbers LIMIT 30)
      GROUP BY grouping, item
      ORDER BY grouping, item
);
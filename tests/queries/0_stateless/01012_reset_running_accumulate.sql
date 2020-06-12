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
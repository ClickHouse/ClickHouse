-- Exercise AggregateFunctionSequenceNextNode direction/base combinations,
-- multiple event matchers, partitioned aggregation via GROUP BY, and error paths.

SET allow_experimental_funnel_functions = 1;

SELECT '--- forward + head ---';
SELECT sequenceNextNode('forward', 'head')(time, event, event = 'A')
FROM (SELECT toDateTime('2020-01-01 00:00:0' || toString(number)) AS time,
             arrayElement(['A','B','C','A','B','C'], number+1) AS event
      FROM numbers(6));

SELECT '--- backward + tail ---';
SELECT sequenceNextNode('backward', 'tail')(time, event, event = 'A')
FROM (SELECT toDateTime('2020-01-01 00:00:0' || toString(number)) AS time,
             arrayElement(['A','B','C','A','B','C'], number+1) AS event
      FROM numbers(6));

SELECT '--- forward + first_match (two matchers; require both) ---';
SELECT sequenceNextNode('forward', 'first_match')(time, event, event = 'A', event = 'B')
FROM (SELECT toDateTime('2020-01-01 00:00:0' || toString(number)) AS time,
             arrayElement(['X','A','B','C','A','B'], number+1) AS event
      FROM numbers(6));

SELECT '--- forward + last_match ---';
SELECT sequenceNextNode('forward', 'last_match')(time, event, event = 'A', event = 'B')
FROM (SELECT toDateTime('2020-01-01 00:00:0' || toString(number)) AS time,
             arrayElement(['X','A','B','C','A','B'], number+1) AS event
      FROM numbers(6));

SELECT '--- backward + first_match ---';
SELECT sequenceNextNode('backward', 'first_match')(time, event, event = 'A', event = 'B')
FROM (SELECT toDateTime('2020-01-01 00:00:0' || toString(number)) AS time,
             arrayElement(['A','B','C','A','B','X'], number+1) AS event
      FROM numbers(6));

SELECT '--- backward + last_match ---';
SELECT sequenceNextNode('backward', 'last_match')(time, event, event = 'A', event = 'B')
FROM (SELECT toDateTime('2020-01-01 00:00:0' || toString(number)) AS time,
             arrayElement(['A','B','C','A','B','X'], number+1) AS event
      FROM numbers(6));

SELECT '--- partitioned with GROUP BY (merge path) ---';
SELECT p, sequenceNextNode('forward', 'head')(time, event, event = 'A')
FROM (SELECT arrayElement(['p1','p2'], (number % 2) + 1) AS p,
             toDateTime('2020-01-01 00:00:0' || toString(number)) AS time,
             arrayElement(['A','B','C','A','B','C'], number+1) AS event
      FROM numbers(6))
GROUP BY p
ORDER BY p;

SELECT '--- too few events (<= events_size): NULL ---';
SELECT sequenceNextNode('forward', 'head')(time, event, event = 'A', event = 'B', event = 'C')
FROM (SELECT toDateTime('2020-01-01 00:00:00') AS time, 'A' AS event);

SELECT '--- empty input ---';
SELECT sequenceNextNode('forward', 'head')(time, event, event = 'A')
FROM (SELECT toDateTime('2020-01-01') AS time, 'A' AS event WHERE 0);

SELECT '--- error: invalid direction ---';
SELECT sequenceNextNode('bad', 'head')(time, event, event = 'A')
FROM (SELECT toDateTime('2020-01-01') AS time, 'A' AS event); -- { serverError BAD_ARGUMENTS }

SELECT '--- error: invalid base ---';
SELECT sequenceNextNode('forward', 'bad')(time, event, event = 'A')
FROM (SELECT toDateTime('2020-01-01') AS time, 'A' AS event); -- { serverError BAD_ARGUMENTS }

SELECT '--- error: forward + tail is rejected ---';
SELECT sequenceNextNode('forward', 'tail')(time, event, event = 'A')
FROM (SELECT toDateTime('2020-01-01') AS time, 'A' AS event); -- { serverError BAD_ARGUMENTS }

SELECT '--- error: backward + head is rejected ---';
SELECT sequenceNextNode('backward', 'head')(time, event, event = 'A')
FROM (SELECT toDateTime('2020-01-01') AS time, 'A' AS event); -- { serverError BAD_ARGUMENTS }

SELECT '--- error: event column must be String ---';
SELECT sequenceNextNode('forward', 'head')(time, event, event = 1)
FROM (SELECT toDateTime('2020-01-01') AS time, 1 AS event); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- max_args_function: max_events=32 allowed ---';
SELECT sequenceNextNode('forward', 'first_match')(time, event, event = 'A',
    event = 'B', event = 'C', event = 'D', event = 'E', event = 'F', event = 'G', event = 'H')
FROM (SELECT toDateTime('2020-01-01 00:00:0' || toString(number)) AS time,
             arrayElement(['A','B','C','D','E','F','G','H'], number+1) AS event
      FROM numbers(8));

SELECT '--- -Distinct aggregator combinator ---';
SELECT sequenceNextNodeDistinct('forward','head')(time, event, event = 'A')
FROM (SELECT toDateTime('2020-01-01 00:00:0' || toString(number)) AS time,
             arrayElement(['A','B','A','B','A','B'], number+1) AS event
      FROM numbers(6));

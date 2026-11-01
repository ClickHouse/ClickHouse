-- https://github.com/ClickHouse/ClickHouse/issues/123756: with `use_aggregation_memory_tracker = 0` the two-level conversion of `GROUP BY` also counts the memory held by the rest of the query.

SET max_threads = 1;
SET max_untracked_memory = 0;
SET max_bytes_before_external_group_by = 1000000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 100000;

SELECT r.k AS g, groupArray(l.s)
FROM (SELECT number AS id, leftPad(toString(number), 10, '0') AS s FROM numbers(1000)) AS l
JOIN (SELECT number AS id, toFixedString(toString(number % 10), 64) AS k FROM numbers(1000)) AS r
ON l.id = r.id
GROUP BY g
FORMAT Null
SETTINGS log_comment = 'aggregation_memory_tracker';

SELECT r.k AS g, groupArray(l.s)
FROM (SELECT number AS id, leftPad(toString(number), 10, '0') AS s FROM numbers(1000)) AS l
JOIN (SELECT number AS id, toFixedString(toString(number % 10), 64) AS k FROM numbers(1000)) AS r
ON l.id = r.id
GROUP BY g
FORMAT Null
SETTINGS log_comment = 'query_memory', use_aggregation_memory_tracker = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['AggregationConvertedToTwoLevel'] > 0 AS two_level
FROM system.query_log
WHERE log_comment IN ('aggregation_memory_tracker', 'query_memory')
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
ORDER BY log_comment
SETTINGS max_rows_to_read = 0;

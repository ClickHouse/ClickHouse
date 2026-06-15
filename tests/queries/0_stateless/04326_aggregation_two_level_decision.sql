-- Test the memory tracking for conversion of group by to two level.
-- Only the aggregation state should be taken into account, independent
-- of the memory held by the rest of the query.

-- Pin settings necessary for two-level conversion to trigger.
SET max_threads = 1;
SET max_untracked_memory = 0;
SET max_bytes_ratio_before_external_group_by = 0.5;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 100000;

-- Small aggregate state over join stays single-level.
SELECT r.k AS g, groupArray(l.s)
FROM (SELECT number AS id, leftPad(toString(number), 10, '0') AS s FROM numbers(1000)) AS l
JOIN (SELECT number AS id, toFixedString(toString(number % 10), 64) AS k FROM numbers(1000)) AS r
ON l.id = r.id
GROUP BY g
FORMAT Null
SETTINGS log_comment = 'aggregation_small_state';

-- Large aggregate state over join is converted to two-level.
SELECT r.k AS g, groupArray(l.s)
FROM (SELECT number AS id, leftPad(toString(number), 10000, '0') AS s FROM numbers(1000)) AS l
JOIN (SELECT number AS id, toFixedString(toString(number % 10), 64) AS k FROM numbers(1000)) AS r
ON l.id = r.id
GROUP BY g
FORMAT Null
SETTINGS log_comment = 'aggregation_large_state';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT q.log_comment,
       countIf(t.message_format_string LIKE '%Converting aggregation data to two-level%') > 0 AS two_level
FROM system.query_log q
LEFT JOIN system.text_log t ON t.query_id = q.query_id
WHERE q.log_comment IN ('aggregation_small_state', 'aggregation_large_state')
  AND q.type = 'QueryFinish'
  AND q.current_database = currentDatabase()
GROUP BY q.log_comment
ORDER BY q.log_comment
SETTINGS max_rows_to_read = 0;

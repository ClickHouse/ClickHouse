-- A non-trivial analyzer-path query so every phase reliably spends > 0 microseconds.
-- Marker comment 'query_phase_pe_marker' lets us locate the row in query_log.
SELECT a.k AS k, count() AS c -- query_phase_pe_marker
FROM
(
    SELECT number, number % 10 AS k FROM numbers(2000)
) AS a
ALL INNER JOIN
(
    SELECT number AS k2, number * 2 AS v FROM numbers(200)
) AS b
ON a.k = b.k2
WHERE a.number > 5
GROUP BY a.k
ORDER BY a.k
SETTINGS enable_analyzer = 1
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['QueryAnalysisMicroseconds'] > 0,
    ProfileEvents['QueryTreeOptimizeMicroseconds'] > 0,
    ProfileEvents['QueryPlanningMicroseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%query_phase_pe_marker%'
  AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

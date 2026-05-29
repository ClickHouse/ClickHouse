-- Issue #21697: INSERT ... RETURNING must record the subquery's table/column access in system.query_log.
-- The RETURNING subquery reads a table other than the INSERT target; that read should show up in `tables`.

SET async_insert = 0;

DROP TABLE IF EXISTS t_returning_access_src;
DROP TABLE IF EXISTS t_returning_access_dst;

CREATE TABLE t_returning_access_src (id UInt64) ENGINE = Memory;
CREATE TABLE t_returning_access_dst (id UInt64) ENGINE = Memory;

INSERT INTO t_returning_access_src VALUES (7);

-- The RETURNING subquery result (7) is emitted as the statement's output.
INSERT INTO t_returning_access_dst (id) RETURNING (SELECT id FROM t_returning_access_src WHERE id = 7) VALUES (1);

SYSTEM FLUSH LOGS query_log;

SELECT arrayExists(t -> t LIKE '%t_returning_access_src%', tables) AS returning_source_recorded
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 600 SECOND
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE 'INSERT INTO t_returning_access_dst%RETURNING%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_returning_access_dst;
DROP TABLE t_returning_access_src;

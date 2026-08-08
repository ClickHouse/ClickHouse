-- Tags: no-parallel, no-fasttest

DROP TABLE IF EXISTS to_insert;
CREATE TABLE to_insert (value UInt64) ENGINE = Memory();

INSERT INTO table_that_do_not_exists VALUES (42); -- { serverError UNKNOWN_TABLE }
INSERT INTO to_insert SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT * FROM table_that_do_not_exists; -- { serverError UNKNOWN_TABLE }
SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SYSTEM FLUSH LOGS query_log;

SELECT normalizeQuery(query), type, ProfileEvents['FailedSelectQuery'], ProfileEvents['FailedInsertQuery']
FROM system.query_log
WHERE current_database = currentDatabase() AND query_kind IN ('Select', 'Insert') AND event_date >= yesterday() AND type != 'QueryStart'
ORDER BY event_time_microseconds;

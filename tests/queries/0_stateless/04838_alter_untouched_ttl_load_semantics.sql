-- A `TTL` the `ALTER` did not touch is rebuilt from stored metadata, so it is rebuilt with the same
-- semantics the server uses when it loads the table: whatever loads on every start must survive an
-- unrelated `ALTER`, even in a session without the settings the `TTL` was created under.

DROP TABLE IF EXISTS t_untouched_table_ttl;
DROP TABLE IF EXISTS t_untouched_column_ttl;
DROP TABLE IF EXISTS t_ttl_broken_by_alter;

SET allow_suspicious_ttl_expressions = 1;

-- A constant table `TTL`: accepted only with `allow_suspicious_ttl_expressions`, and loaded on every start.
CREATE TABLE t_untouched_table_ttl (x UInt32) ENGINE = MergeTree ORDER BY tuple() TTL toDate('2020-01-01');

-- The same for a column `TTL`.
CREATE TABLE t_untouched_column_ttl (d DateTime, x UInt32 TTL toDate('2020-01-01')) ENGINE = MergeTree ORDER BY tuple();

SET allow_suspicious_ttl_expressions = 0;

-- Unrelated `ALTER`s must not revalidate the stored `TTL`.
ALTER TABLE t_untouched_table_ttl ADD COLUMN y UInt8;
ALTER TABLE t_untouched_column_ttl ADD COLUMN y UInt8;
ALTER TABLE t_untouched_table_ttl COMMENT COLUMN x 'unrelated';

SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_untouched_table_ttl' AND name = 'y';
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_untouched_column_ttl' AND name = 'y';

-- A `TTL` the `ALTER` does change is fresh user input and keeps the strict validation.
ALTER TABLE t_untouched_table_ttl MODIFY TTL toDate('2021-01-01'); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_untouched_column_ttl MODIFY COLUMN x UInt32 TTL toDate('2021-01-01'); -- { serverError BAD_ARGUMENTS }

-- The stored `TTL` is still rebuilt against the new columns and still has to produce a date: an `ALTER`
-- may not break it. Load semantics relax the suspicious-`TTL` policy checks, nothing else.
CREATE TABLE t_ttl_broken_by_alter (d DateTime, x UInt32) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 DAY;
ALTER TABLE t_ttl_broken_by_alter MODIFY COLUMN d UInt32; -- { serverError BAD_TTL_EXPRESSION, ALTER_OF_COLUMN_IS_FORBIDDEN, ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_untouched_table_ttl;
DROP TABLE t_untouched_column_ttl;
DROP TABLE t_ttl_broken_by_alter;

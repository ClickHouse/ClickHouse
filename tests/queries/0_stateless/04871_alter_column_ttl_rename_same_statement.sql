-- A column `TTL` set and renamed by the same `ALTER` statement is fresh user input: the set of columns
-- whose `TTL` a command changed is matched against the final column names (after all commands applied),
-- so a `RENAME COLUMN` following the command must carry the mark to the new name - otherwise the fresh
-- `TTL` would be rebuilt with metadata-load semantics and skip the fresh-DDL suspicious-`TTL` validation.

DROP TABLE IF EXISTS t_ttl_rename_same_alter;

CREATE TABLE t_ttl_rename_same_alter (d DateTime, x UInt32) ENGINE = MergeTree ORDER BY tuple();

-- A suspicious (constant) column `TTL` cannot slip through the validation by renaming the column it was
-- added on in the same statement.
ALTER TABLE t_ttl_rename_same_alter ADD COLUMN c UInt32 TTL toDate('2020-01-01'), RENAME COLUMN c TO e; -- { serverError BAD_ARGUMENTS }

-- (Modifying and renaming the same column in one statement is rejected up front.)
ALTER TABLE t_ttl_rename_same_alter MODIFY COLUMN x UInt32 TTL toDate('2020-01-01'), RENAME COLUMN x TO y; -- { serverError NOT_IMPLEMENTED }

-- The failed statements left no trace.
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_ttl_rename_same_alter' ORDER BY name;

-- With `allow_suspicious_ttl_expressions` the combined statement is accepted, and the TTL lands on the
-- renamed column.
SET allow_suspicious_ttl_expressions = 1;
ALTER TABLE t_ttl_rename_same_alter ADD COLUMN c UInt32 TTL toDate('2020-01-01'), RENAME COLUMN c TO e;
SELECT create_table_query LIKE '%`e` UInt32 TTL toDate(\'2020-01-01\')%'
FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_rename_same_alter';

DROP TABLE t_ttl_rename_same_alter;

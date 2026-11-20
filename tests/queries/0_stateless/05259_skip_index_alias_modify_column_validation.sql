-- Existing skip indexes are rebuilt without the fresh-definition checks on `ALTER`, so grandfathered
-- metadata keeps working. But when the `ALTER` changes an `ALIAS` column that an index expands through,
-- the expanded expression is fresh input: an alias hiding an `IN` over a table must be rejected.

DROP TABLE IF EXISTS t_index_alias;
DROP TABLE IF EXISTS t_set;

CREATE TABLE t_set (x UInt64) ENGINE = Set;
CREATE TABLE t_index_alias (x UInt64, a ALIAS x, INDEX idx a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY x;
INSERT INTO t_index_alias VALUES (1), (2);

ALTER TABLE t_index_alias MODIFY COLUMN a ALIAS x IN t_set; -- { serverError BAD_ARGUMENTS }
SELECT create_table_query LIKE '%t_set%' FROM system.tables WHERE database = currentDatabase() AND name = 't_index_alias';

-- Changing the alias to another valid expression is still allowed.
ALTER TABLE t_index_alias MODIFY COLUMN a ALIAS x + 1;
SELECT x, a FROM t_index_alias ORDER BY x;

-- An unrelated `ALTER` keeps working.
ALTER TABLE t_index_alias ADD COLUMN y UInt64 DEFAULT 0;
SELECT x, a, y FROM t_index_alias ORDER BY x;

DROP TABLE t_index_alias;
DROP TABLE t_set;

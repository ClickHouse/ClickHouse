-- Tags: no-replicated-database
-- no-replicated-database: the guards tested here are intentionally kept on replicated tables
-- (the local part state cannot prove that the table is empty on all replicas), and in that run
-- plain MergeTree tables are created as replicated.

-- The guards protecting data in existing parts (stale skip-index files under
-- `alter_column_secondary_index_mode = 'throw'` / `'compatibility'`, rematerialization of
-- `MATERIALIZED` columns in the sort key or depending on `EPHEMERAL` columns) must not reject
-- ALTERs on a table with no parts: there is nothing to rewrite, the ALTER is metadata-only and
-- only affects future inserts.

SET alter_sync = 2;
SET mutations_sync = 2;

SELECT '-- skip index over an ALIAS: allowed while empty, rejected once parts exist';
DROP TABLE IF EXISTS t_empty_index;
CREATE TABLE t_empty_index
(
    a UInt64,
    x UInt64 ALIAS a + 1,
    INDEX idx x TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY a SETTINGS alter_column_secondary_index_mode = 'throw';

ALTER TABLE t_empty_index MODIFY COLUMN x UInt64 ALIAS a + 2;

INSERT INTO t_empty_index (a) VALUES (1);
ALTER TABLE t_empty_index MODIFY COLUMN x UInt64 ALIAS a + 3; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT a, x FROM t_empty_index;

DROP TABLE t_empty_index;

SELECT '-- MATERIALIZED depending on EPHEMERAL: allowed while empty, no mutation queued';
DROP TABLE IF EXISTS t_empty_ephemeral;
CREATE TABLE t_empty_ephemeral
(
    a UInt64,
    e UInt64 EPHEMERAL 7,
    m UInt64 MATERIALIZED greatest(a, * EXCEPT m) + e
) ENGINE = MergeTree ORDER BY a;

ALTER TABLE t_empty_ephemeral ADD COLUMN b UInt64 DEFAULT a + 1000;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_empty_ephemeral' AND command ILIKE '%MATERIALIZE COLUMN%';

-- Future inserts use the new expansion.
INSERT INTO t_empty_ephemeral (a) VALUES (1);
SELECT a, b, m FROM t_empty_ephemeral;

-- Once a part exists, the same kind of ALTER is rejected again.
ALTER TABLE t_empty_ephemeral ADD COLUMN c UInt64 DEFAULT a + 2000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_empty_ephemeral;

SELECT '-- MATERIALIZED in the sort key: allowed while empty, rejected once parts exist';
DROP TABLE IF EXISTS t_empty_sort_key;
CREATE TABLE t_empty_sort_key
(
    a UInt64,
    m UInt64 MATERIALIZED greatest(a, * EXCEPT m)
) ENGINE = MergeTree ORDER BY (a, m);

ALTER TABLE t_empty_sort_key ADD COLUMN b UInt64 DEFAULT a + 1000;

INSERT INTO t_empty_sort_key (a) VALUES (1);
SELECT a, b, m FROM t_empty_sort_key;

ALTER TABLE t_empty_sort_key ADD COLUMN c UInt64 DEFAULT a + 2000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_empty_sort_key;

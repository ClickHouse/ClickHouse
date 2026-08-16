-- Tags: no-replicated-database
-- no-replicated-database: the guards tested here are intentionally kept on replicated tables
-- (the local part state cannot prove that the table is empty on all replicas), and in that run
-- plain MergeTree tables are created as replicated.

-- An insert can start under the old metadata and commit the first part after an ALTER sees no
-- active parts. The guards must therefore remain conservative even on an empty plain MergeTree.

SET alter_sync = 2;
SET mutations_sync = 2;

SELECT '-- skip index over an ALIAS: rejected even while empty';
DROP TABLE IF EXISTS t_empty_index;
CREATE TABLE t_empty_index
(
    a UInt64,
    x UInt64 ALIAS a + 1,
    INDEX idx x TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY a SETTINGS alter_column_secondary_index_mode = 'throw';

ALTER TABLE t_empty_index MODIFY COLUMN x UInt64 ALIAS a + 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_empty_index;

SELECT '-- MATERIALIZED depending on EPHEMERAL: rejected even while empty';
DROP TABLE IF EXISTS t_empty_ephemeral;
CREATE TABLE t_empty_ephemeral
(
    a UInt64,
    e UInt64 EPHEMERAL 7,
    m UInt64 MATERIALIZED greatest(a, * EXCEPT m) + e
) ENGINE = MergeTree ORDER BY a;

ALTER TABLE t_empty_ephemeral ADD COLUMN b UInt64 DEFAULT a + 1000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_empty_ephemeral;

SELECT '-- MATERIALIZED in the sort key: rejected even while empty';
DROP TABLE IF EXISTS t_empty_sort_key;
CREATE TABLE t_empty_sort_key
(
    a UInt64,
    m UInt64 MATERIALIZED greatest(a, * EXCEPT m)
) ENGINE = MergeTree ORDER BY (a, m);

ALTER TABLE t_empty_sort_key ADD COLUMN b UInt64 DEFAULT a + 1000; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_empty_sort_key;

-- Tags: no-random-merge-tree-settings
-- Tag no-random-merge-tree-settings: randomized settings are injected into the `CREATE` of the control table

-- Resetting `auto_statistics_types` has to rebuild the implicit statistics from the default types, in every spelling.
-- The expected types are not spelled out: the table under test is compared with a control table created with the default.

SET allow_statistics = 1;

DROP TABLE IF EXISTS t_reset_statistics_control;
DROP TABLE IF EXISTS t_reset_statistics;

CREATE TABLE t_reset_statistics_control (id UInt64, s String) ENGINE = MergeTree ORDER BY id;

CREATE TABLE t_reset_statistics (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'tdigest';

SELECT 'distinct statistics before the reset', uniqExact(statistics_of_table) FROM
(
    SELECT arraySort(groupArray((name, statistics))) AS statistics_of_table
    FROM system.columns WHERE database = currentDatabase() AND table LIKE 't_reset_statistics%' GROUP BY table
);

ALTER TABLE t_reset_statistics RESET SETTING auto_statistics_types;

SELECT 'distinct statistics after RESET SETTING', uniqExact(statistics_of_table) FROM
(
    SELECT arraySort(groupArray((name, statistics))) AS statistics_of_table
    FROM system.columns WHERE database = currentDatabase() AND table LIKE 't_reset_statistics%' GROUP BY table
);

ALTER TABLE t_reset_statistics MODIFY SETTING auto_statistics_types = 'tdigest';
ALTER TABLE t_reset_statistics MODIFY SETTING auto_statistics_types = DEFAULT;

SELECT 'distinct statistics after MODIFY SETTING = DEFAULT', uniqExact(statistics_of_table) FROM
(
    SELECT arraySort(groupArray((name, statistics))) AS statistics_of_table
    FROM system.columns WHERE database = currentDatabase() AND table LIKE 't_reset_statistics%' GROUP BY table
);

DROP TABLE t_reset_statistics_control;
DROP TABLE t_reset_statistics;

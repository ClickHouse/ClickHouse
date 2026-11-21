-- Focused test for the size gate of insert-time statistics materialization.
-- A part written by INSERT carries statistics only when the table's current active size
-- plus the size of the inserted block does not exceed `materialize_statistics_on_insert_max_table_size`
-- (`0` means no size limit). The gate is evaluated per part, against the table size at the time of the INSERT.

SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET insert_deduplicate = 0;
SET async_insert = 0;

DROP TABLE IF EXISTS t_stats_size_gate;

CREATE TABLE t_stats_size_gate (a UInt64 STATISTICS(basic, uniq)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '';

SYSTEM STOP MERGES t_stats_size_gate;

-- (1) Empty table, but the inserted block alone is larger than the cap: no statistics.
INSERT INTO t_stats_size_gate SETTINGS materialize_statistics_on_insert_max_table_size = 1 SELECT number FROM numbers(1000);

-- (2) Zero cap means no size limit: statistics are built.
INSERT INTO t_stats_size_gate SETTINGS materialize_statistics_on_insert_max_table_size = 0 SELECT number + 1000 FROM numbers(1000);

-- (3) Small table, still below the cap of 1000000 bytes: statistics are built.
INSERT INTO t_stats_size_gate SETTINGS materialize_statistics_on_insert_max_table_size = 1000000 VALUES (2000);

-- (4) Grow the table beyond 1000000 bytes with incompressible data (no cap here, so this part gets statistics too).
INSERT INTO t_stats_size_gate SETTINGS materialize_statistics_on_insert_max_table_size = 0 SELECT rand64() FROM numbers(500000);

-- (5) The same one-row insert as (3) now skips statistics: the table size alone exceeds the cap.
INSERT INTO t_stats_size_gate SETTINGS materialize_statistics_on_insert_max_table_size = 1000000 VALUES (2001);

-- (6) With the setting disabled, no statistics are built regardless of the cap.
INSERT INTO t_stats_size_gate SETTINGS materialize_statistics_on_insert = 0, materialize_statistics_on_insert_max_table_size = 0 SELECT number + 3000 FROM numbers(10);

SELECT statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_size_gate' AND active AND column = 'a'
ORDER BY name;

DROP TABLE t_stats_size_gate;

SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET insert_keeper_fault_injection_probability = 0.0;

DROP TABLE IF EXISTS t_part_stats_wide;
DROP TABLE IF EXISTS t_part_stats_compact;
DROP TABLE IF EXISTS t_part_stats_none;

CREATE TABLE t_part_stats_wide
(
    id UInt64 STATISTICS(minmax, uniq),
    n Nullable(Int64) STATISTICS(basic),
    s String STATISTICS(uniq),
    s2 String STATISTICS(basic),
    t Float64 STATISTICS(tdigest),
    no_stats String
)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES t_part_stats_wide;
INSERT INTO t_part_stats_wide SELECT number, if(number % 10 = 0, NULL, number), toString(number % 100), toString(number), number / 7, toString(number) FROM numbers(1000);
INSERT INTO t_part_stats_wide SELECT number, NULL, 'x', 'y', 3.14, 'z' FROM numbers(1000, 500);

SELECT name, column, type, statistics, rows, min, max, cardinality, null_count
FROM system.part_statistics
WHERE database = currentDatabase() AND table = 't_part_stats_wide' AND active
ORDER BY name, column;

SELECT 'compact';
CREATE TABLE t_part_stats_compact (id UInt64 STATISTICS(minmax), s String STATISTICS(uniq))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0,
         min_bytes_for_wide_part = 100000000, min_rows_for_wide_part = 100000000;
INSERT INTO t_part_stats_compact SELECT number, toString(number % 3) FROM numbers(100);

SELECT column, type, statistics, rows, min, max, cardinality
FROM system.part_statistics
WHERE database = currentDatabase() AND table = 't_part_stats_compact' AND active
ORDER BY column;

SELECT 'no stats';
CREATE TABLE t_part_stats_none (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '';
INSERT INTO t_part_stats_none SELECT number FROM numbers(10);
SELECT count() FROM system.part_statistics WHERE database = currentDatabase() AND table = 't_part_stats_none';

DROP TABLE t_part_stats_wide;
DROP TABLE t_part_stats_compact;
DROP TABLE t_part_stats_none;

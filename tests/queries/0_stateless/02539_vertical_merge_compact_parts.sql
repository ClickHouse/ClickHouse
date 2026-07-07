DROP TABLE IF EXISTS t_compact_vertical_merge;

CREATE TABLE t_compact_vertical_merge (id UInt64, s LowCardinality(String), arr Array(UInt64))
ENGINE MergeTree ORDER BY id
SETTINGS
    index_granularity = 16,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 100,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    min_bytes_for_full_part_storage = 0;

INSERT INTO t_compact_vertical_merge SELECT number, toString(number), range(number % 10) FROM numbers(40);
INSERT INTO t_compact_vertical_merge SELECT number, toString(number), range(number % 10) FROM numbers(40);

OPTIMIZE TABLE t_compact_vertical_merge FINAL;
SYSTEM FLUSH LOGS part_log;

WITH splitByChar('_', part_name) AS name_parts,
    name_parts[2]::UInt64 AS min_block,
    name_parts[3]::UInt64 AS max_block
SELECT min_block, max_block, event_type, merge_algorithm, part_type FROM system.part_log
WHERE
    database = currentDatabase() AND
    table = 't_compact_vertical_merge' AND
    min_block = 1 AND max_block = 2
ORDER BY event_time_microseconds;

INSERT INTO t_compact_vertical_merge SELECT number, toString(number), range(number % 10) FROM numbers(40);

OPTIMIZE TABLE t_compact_vertical_merge FINAL;
SYSTEM FLUSH LOGS part_log;

WITH splitByChar('_', part_name) AS name_parts,
    name_parts[2]::UInt64 AS min_block,
    name_parts[3]::UInt64 AS max_block
SELECT min_block, max_block, event_type, merge_algorithm, part_type FROM system.part_log
WHERE
    database = currentDatabase() AND
    table = 't_compact_vertical_merge' AND
    min_block = 1 AND max_block = 3
ORDER BY event_time_microseconds;

DROP TABLE t_compact_vertical_merge;

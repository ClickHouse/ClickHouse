-- Vertical merge into a `Wide` part with `Packed` storage. Every gathered column is written by its own
-- writer into the same in-memory archive, which rejects a second write of the same file, so the shared
-- offsets of `Nested` must be written only once. Skip indexes over gathered columns are built by the
-- per-column writers, while an index over several columns is built in the horizontal stage.
-- The result is compared with a horizontal merge of the same data into a `Full` part.

SET flatten_nested = 1;

DROP TABLE IF EXISTS t_vertical_packed;
DROP TABLE IF EXISTS t_horizontal_full;

CREATE TABLE t_vertical_packed
(
    id UInt64,
    s String,
    lc LowCardinality(String),
    arr Array(UInt64),
    n Nested(a UInt64, b String),
    m Map(String, UInt64),
    sparse UInt64,
    INDEX idx_s s TYPE bloom_filter GRANULARITY 1,
    INDEX idx_sum id + sparse TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = '100G',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

CREATE TABLE t_horizontal_full AS t_vertical_packed
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 0;

SYSTEM STOP MERGES t_vertical_packed;

-- Interleaved keys, so that the merge takes consecutive rows from different source parts.
INSERT INTO t_vertical_packed SELECT
    number * 3 AS id, 'str_' || toString(id), toString(id % 17), range(id % 5),
    range(id % 4), arrayMap(x -> toString(x), range(id % 4)), map('k' || toString(id % 3), id), if(id % 100 = 0, id, 0)
FROM numbers(300);

INSERT INTO t_vertical_packed SELECT
    number * 3 + 1 AS id, 'str_' || toString(id), toString(id % 17), range(id % 5),
    range(id % 4), arrayMap(x -> toString(x), range(id % 4)), map('k' || toString(id % 3), id), if(id % 100 = 0, id, 0)
FROM numbers(300);

INSERT INTO t_vertical_packed SELECT
    number * 3 + 2 AS id, 'str_' || toString(id), toString(id % 17), range(id % 5),
    range(id % 4), arrayMap(x -> toString(x), range(id % 4)), map('k' || toString(id % 3), id), if(id % 100 = 0, id, 0)
FROM numbers(300);

INSERT INTO t_horizontal_full SELECT * FROM t_vertical_packed;

SELECT 'before merge';
SELECT part_type, part_storage_type, count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_vertical_packed' AND active
GROUP BY part_type, part_storage_type;

SYSTEM START MERGES t_vertical_packed;
OPTIMIZE TABLE t_vertical_packed FINAL;
OPTIMIZE TABLE t_horizontal_full FINAL;

SELECT 'after merge';
SELECT part_type, part_storage_type, count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_vertical_packed' AND active
GROUP BY part_type, part_storage_type;

SYSTEM FLUSH LOGS part_log;

SELECT DISTINCT merge_algorithm, part_type FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 't_vertical_packed' AND event_type = 'MergeParts';

SELECT (SELECT count(), sum(cityHash64(*)) FROM t_vertical_packed) = (SELECT count(), sum(cityHash64(*)) FROM t_horizontal_full);
SELECT count() FROM t_vertical_packed WHERE s = 'str_300' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT count() FROM t_vertical_packed WHERE id + sparse = 600 SETTINGS force_data_skipping_indices = 'idx_sum';

CHECK TABLE t_vertical_packed SETTINGS check_query_single_value_result = 1;

-- The part must also be readable after it is loaded from the archive on disk.
DETACH TABLE t_vertical_packed;
ATTACH TABLE t_vertical_packed;

SELECT 'after attach';
SELECT part_type, part_storage_type, count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_vertical_packed' AND active
GROUP BY part_type, part_storage_type;

SELECT (SELECT count(), sum(cityHash64(*)) FROM t_vertical_packed) = (SELECT count(), sum(cityHash64(*)) FROM t_horizontal_full);
SELECT count() FROM t_vertical_packed WHERE s = 'str_300' SETTINGS force_data_skipping_indices = 'idx_s';

DROP TABLE t_vertical_packed;
DROP TABLE t_horizontal_full;

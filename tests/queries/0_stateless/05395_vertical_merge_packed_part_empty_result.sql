-- Vertical merge into a `Wide` part with `Packed` storage when all rows collapse and the result is empty.
-- Without rows the writers skip the final mark, but they still create the streams of their columns, so the
-- shared offsets of `Nested` have to be registered anyway. Otherwise the writer of the next subcolumn
-- creates the same file in the archive again, which the archive rejects.

SET flatten_nested = 1;

DROP TABLE IF EXISTS t_vertical_packed_empty;

CREATE TABLE t_vertical_packed_empty
(
    id UInt64,
    sign Int8,
    s String,
    n Nested(a UInt64, b String, c Array(UInt64))
)
ENGINE = CollapsingMergeTree(sign) ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = '100G',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_vertical_packed_empty;

INSERT INTO t_vertical_packed_empty VALUES (1, 1, 'x', [1, 2], ['a', 'b'], [[1], [2, 3]]);
INSERT INTO t_vertical_packed_empty VALUES (1, -1, 'x', [1, 2], ['a', 'b'], [[1], [2, 3]]);

SELECT part_type, part_storage_type, count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_vertical_packed_empty' AND active
GROUP BY part_type, part_storage_type;

SYSTEM START MERGES t_vertical_packed_empty;
OPTIMIZE TABLE t_vertical_packed_empty FINAL;

SELECT count() FROM t_vertical_packed_empty;

SYSTEM FLUSH LOGS part_log;

SELECT DISTINCT merge_algorithm, part_type, rows, error FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 't_vertical_packed_empty' AND event_type = 'MergeParts';

DROP TABLE t_vertical_packed_empty;

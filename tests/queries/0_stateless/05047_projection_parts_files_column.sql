-- Test for the `files` column in `system.projection_parts`.
-- https://github.com/ClickHouse/ClickHouse/issues/115133

DROP TABLE IF EXISTS t_projection_parts_files;

CREATE TABLE t_projection_parts_files
(
    key UInt64,
    value UInt64,
    PROJECTION proj
    (
        SELECT value, key
        ORDER BY value
    )
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;

INSERT INTO t_projection_parts_files SELECT number, number * 2 FROM numbers(100);

-- The projection part is Compact (pinned by the table settings above), so its checksums
-- contain exactly: count.txt, data.bin, marks, primary index and serialization.json.
SELECT name, files
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_projection_parts_files' AND active;

DROP TABLE t_projection_parts_files;

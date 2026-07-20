CREATE TABLE 03593_backup_with_broken_projection
(
    `id` UInt64,
    `string` String,
    `time1` DateTime64(6),
    `time2` DateTime64(6),
    PROJECTION max_time
    (
        SELECT
            string,
            max(time1),
            max(time2)
        GROUP BY string
    )
)
ENGINE = MergeTree
ORDER BY time1;

INSERT INTO 03593_backup_with_broken_projection
SETTINGS max_block_size = 10000000, min_insert_block_size_rows = 10000000
SELECT
    number = 4000000,
    'test',
    '2025-08-11',
    '2025-08-11'
FROM system.numbers
LIMIT 5000000;

ALTER TABLE 03593_backup_with_broken_projection
    (UPDATE _row_exists = 0 WHERE id = 0) SETTINGS mutations_sync=1;

ALTER TABLE 03593_backup_with_broken_projection
    (UPDATE _row_exists = 0 WHERE id = 0) SETTINGS mutations_sync=1;

BACKUP TABLE 03593_backup_with_broken_projection TO Null SETTINGS allow_backup_broken_projections = true, check_projection_parts = false FORMAT Null;
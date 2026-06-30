-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

DROP TABLE IF EXISTS nullable_array_drop_column_sql;

CREATE TABLE nullable_array_drop_column_sql
(
    id UInt32,
    arr Nullable(Array(UInt32))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO nullable_array_drop_column_sql VALUES (1, [1, 2, 3]), (2, NULL);

ALTER TABLE nullable_array_drop_column_sql DROP COLUMN arr SETTINGS mutations_sync = 2;

SELECT throwIf(sum(id) != 3) FROM nullable_array_drop_column_sql FORMAT Null;

SELECT throwIf(count() != 0)
FROM system.columns
WHERE database = currentDatabase()
  AND table = 'nullable_array_drop_column_sql'
  AND name = 'arr'
FORMAT Null;

DROP TABLE nullable_array_drop_column_sql;

SELECT 'ok';

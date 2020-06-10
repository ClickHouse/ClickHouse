DROP TABLE IF EXISTS minmax_compact;

CREATE TABLE minmax_compact
(
    u64 UInt64,
    i64 Int64,
    i32 Int32
) ENGINE = MergeTree()
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2, min_rows_for_wide_part = 1000000;

INSERT INTO minmax_compact VALUES (0, 2, 1), (1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1), (5, 2, 1), (6, 1, 2), (7, 1, 2), (8, 1, 2), (9, 1, 2);

SET mutations_sync = 1;
ALTER TABLE minmax_compact ADD INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1;

ALTER TABLE minmax_compact MATERIALIZE INDEX idx IN PARTITION 1;
set max_rows_to_read = 8;
SELECT count() FROM minmax_compact WHERE i64 = 2;

ALTER TABLE minmax_compact MATERIALIZE INDEX idx IN PARTITION 2;
set max_rows_to_read = 6;
SELECT count() FROM minmax_compact WHERE i64 = 2;

ALTER TABLE minmax_compact CLEAR INDEX idx IN PARTITION 1;
ALTER TABLE minmax_compact CLEAR INDEX idx IN PARTITION 2;

SELECT count() FROM minmax_compact WHERE i64 = 2; -- { serverError 158 }

set max_rows_to_read = 10;
SELECT count() FROM minmax_compact WHERE i64 = 2;

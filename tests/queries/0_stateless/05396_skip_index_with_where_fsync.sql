-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings, no-object-storage
-- The marker file of a partial index is synchronized with the rest of the part when `fsync_after_insert` is on:
-- the insert into a table with a partial index makes exactly one more `fsync` than the insert into a table with
-- the same index without a condition (that is the marker).

SET allow_experimental_partial_skip_indexes = 1;

DROP TABLE IF EXISTS t_fsync_plain;
DROP TABLE IF EXISTS t_fsync_partial;

CREATE TABLE t_fsync_plain (id UInt64, flag UInt8, INDEX idx id TYPE minmax) ENGINE = MergeTree ORDER BY tuple()
SETTINGS fsync_after_insert = 1, fsync_part_directory = 0, min_bytes_for_wide_part = 0;
CREATE TABLE t_fsync_partial (id UInt64, flag UInt8, INDEX idx id TYPE minmax WHERE flag = 1) ENGINE = MergeTree ORDER BY tuple()
SETTINGS fsync_after_insert = 1, fsync_part_directory = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_fsync_plain SELECT number, number % 2 FROM numbers(100) SETTINGS async_insert = 0, log_comment = '05396_plain';
INSERT INTO t_fsync_partial SELECT number, number % 2 FROM numbers(100) SETTINGS async_insert = 0, log_comment = '05396_partial';

SYSTEM FLUSH LOGS query_log;

SELECT
    (SELECT ProfileEvents['FileSync'] FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '05396_partial' AND type = 'QueryFinish' AND query_kind = 'Insert' LIMIT 1)
    - (SELECT ProfileEvents['FileSync'] FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '05396_plain' AND type = 'QueryFinish' AND query_kind = 'Insert' LIMIT 1);

DROP TABLE t_fsync_plain;
DROP TABLE t_fsync_partial;

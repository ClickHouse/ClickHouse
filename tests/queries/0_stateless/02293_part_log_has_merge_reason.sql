DROP TABLE IF EXISTS t_part_log_has_merge_type_table;

CREATE TABLE t_part_log_has_merge_type_table
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE = MergeTree() 
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH
SETTINGS min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = true;

INSERT INTO t_part_log_has_merge_type_table VALUES (now(), 1, 'username1');
INSERT INTO t_part_log_has_merge_type_table VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');

OPTIMIZE TABLE t_part_log_has_merge_type_table FINAL;

SYSTEM FLUSH LOGS;

SELECT
    event_type,
    merge_reason 
FROM
    system.part_log
WHERE
        table = 't_part_log_has_merge_type_table'
    AND
        merge_reason = 'TTLDeleteMerge'
    AND
        database = currentDatabase();

DROP TABLE t_part_log_has_merge_type_table;

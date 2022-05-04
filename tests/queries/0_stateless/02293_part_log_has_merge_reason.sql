DROP TABLE IF EXISTS t_part_log_has_merge_type_table;

CREATE TABLE t_part_log_has_merge_type_table
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE = MergeTree() 
ORDER BY tuple()
TTL event_time + toIntervalMonth(3)
SETTINGS min_bytes_for_wide_part = 0, merge_with_ttl_timeout = 1;

INSERT INTO t_part_log_has_merge_type_table VALUES (now(), 1, 'username1');
INSERT INTO t_part_log_has_merge_type_table VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');

OPTIMIZE TABLE t_part_log_has_merge_type_table FINAL;

SYSTEM FLUSH LOGS;

SELECT count(*)
FROM
(
    SELECT
        metadata_modification_time,
        event_time
    FROM system.tables AS l
    INNER JOIN system.part_log AS r
    ON l.name = r.table
    WHERE (l.database = currentDatabase()) AND
          (l.name = 't_part_log_has_merge_type_table') AND
          (r.event_type = 'MergeParts') AND
          (r.merge_reason = 'TTLDeleteMerge')
)
WHERE (metadata_modification_time <= event_time);

DROP TABLE t_part_log_has_merge_type_table;

DROP TABLE IF EXISTS t0;

CREATE TABLE t0(
    key Int32,
    value Int32
)
ENGINE = MergeTree()
ORDER BY key;

INSERT INTO t0 VALUES (1, 1);

ALTER TABLE t0 DELETE WHERE key = 1 SETTINGS mutations_sync = 1;

SELECT is_done, toUnixTimestamp(create_time) <= toUnixTimestamp(finish_time) FROM system.mutations;

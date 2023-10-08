SET allow_experimental_inverted_index=1;

DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    `timestamp` UInt64,
    `s` String,
    INDEX idx s TYPE inverted(3) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO t (s) VALUES ('I am inverted');

SELECT data_version FROM system.parts WHERE database=currentDatabase() AND table='t' AND active=1;

-- do update column synchronously
ALTER TABLE t UPDATE s='I am not inverted' WHERE 1 SETTINGS mutations_sync=1;

SELECT data_version FROM system.parts WHERE database=currentDatabase() AND table='t' AND active=1;

SELECT s FROM t WHERE s LIKE '%inverted%' SETTINGS force_data_skipping_indices='idx';

DROP TABLE t;

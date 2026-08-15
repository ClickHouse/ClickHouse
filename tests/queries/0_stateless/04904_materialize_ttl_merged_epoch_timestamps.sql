-- An epoch-only source part has no representable TTL bounds: zero is the TTL metadata sentinel. Its
-- absent rows-TTL fingerprint must survive a normal merge with a fingerprinted part; otherwise the
-- merged part can incorrectly use the metadata-only `MODIFY TTL` path and retain the epoch row.

SET alter_sync = 2;

DROP TABLE IF EXISTS t_ttl_merged_epoch_row;
CREATE TABLE t_ttl_merged_epoch_row (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d - INTERVAL 1 DAY
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;
SYSTEM STOP TTL MERGES t_ttl_merged_epoch_row;
SYSTEM STOP MERGES t_ttl_merged_epoch_row;

-- The first part's only computed TTL is the epoch. The second part is fingerprinted normally.
INSERT INTO t_ttl_merged_epoch_row VALUES ('1970-01-02 00:00:00');
INSERT INTO t_ttl_merged_epoch_row VALUES ('2100-01-01 00:00:00');
SYSTEM START MERGES t_ttl_merged_epoch_row;
OPTIMIZE TABLE t_ttl_merged_epoch_row FINAL;

ALTER TABLE t_ttl_merged_epoch_row MODIFY TTL d - INTERVAL 23 HOUR;
SELECT d FROM t_ttl_merged_epoch_row;
DROP TABLE t_ttl_merged_epoch_row;

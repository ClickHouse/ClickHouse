-- A row whose rows TTL computes to exactly 1970-01-01 00:00:00 UTC means "no TTL" to the TTL machinery:
-- `ITTLAlgorithm::isTTLExpired` never expires it, and `MergeTreeDataPartTTLInfo::update` excludes it from
-- the stored bounds (a part that holds such a row records the `has_epoch_timestamps` marker instead).
-- A part that mixes such a row with an already expired one therefore stores bounds that are entirely in
-- the past while still holding a row that a scan of the same TTL expression keeps. The shortcuts that
-- classify a part as fully expired from those bounds alone - `all_data_dropped` in `TTLTransform` and in
-- `TTLDeleteFilterTransform`, and the `TTLDrop` merge selector - must not fire for it, or the epoch row is
-- deleted without ever being read.

SET alter_sync = 2;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_epoch_mixed_mutation;
CREATE TABLE t_ttl_epoch_mixed_mutation (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d - INTERVAL 1 DAY
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;
SYSTEM STOP MERGES t_ttl_epoch_mixed_mutation;

-- The first row's TTL is exactly the epoch, the second one's is long expired.
INSERT INTO t_ttl_epoch_mixed_mutation VALUES ('1970-01-02 00:00:00'), ('2020-01-02 00:00:00');

-- The stored bounds describe the expired row only.
SELECT toTimeZone(delete_ttl_info_min, 'UTC'), toTimeZone(delete_ttl_info_max, 'UTC')
    FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_epoch_mixed_mutation' AND active;

-- `SYSTEM STOP MERGES` also blocks mutations, so lift it before the `ALTER`.
SYSTEM START MERGES t_ttl_epoch_mixed_mutation;

-- `MATERIALIZE TTL` must scan the part instead of replacing it with an empty one.
ALTER TABLE t_ttl_epoch_mixed_mutation MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT d FROM t_ttl_epoch_mixed_mutation ORDER BY d;
DROP TABLE t_ttl_epoch_mixed_mutation;

DROP TABLE IF EXISTS t_ttl_epoch_mixed_merge;
CREATE TABLE t_ttl_epoch_mixed_merge (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d - INTERVAL 1 DAY
    SETTINGS min_bytes_for_full_part_storage = 0;
SYSTEM STOP MERGES t_ttl_epoch_mixed_merge;

INSERT INTO t_ttl_epoch_mixed_merge VALUES ('1970-01-02 00:00:00'), ('2020-01-02 00:00:00');

-- The same holds for a TTL merge of that part.
SYSTEM START MERGES t_ttl_epoch_mixed_merge;
OPTIMIZE TABLE t_ttl_epoch_mixed_merge FINAL;
SELECT d FROM t_ttl_epoch_mixed_merge ORDER BY d;
DROP TABLE t_ttl_epoch_mixed_merge;

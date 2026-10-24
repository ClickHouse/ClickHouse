-- An epoch-only source part has no representable TTL bounds: zero is the TTL metadata sentinel, so the
-- part records `has_epoch_timestamps` instead. Its rows TTL IS calculated, though, so a merge must
-- propagate the stored TTL infos of its sources (`MergeTreeDataPartTTLInfos::update`) rather than
-- recalculate them forcefully - and the epoch part's absent rows-TTL fingerprint must survive that
-- propagation; otherwise the merged part could incorrectly use the metadata-only `MODIFY TTL` path
-- and retain the epoch row.

SET alter_sync = 2;

DROP TABLE IF EXISTS t_ttl_merged_epoch_row;
CREATE TABLE t_ttl_merged_epoch_row (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d - INTERVAL 1 DAY
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;
SYSTEM STOP MERGES t_ttl_merged_epoch_row;

-- The first part's only computed TTL is the epoch. The second part is fingerprinted normally.
INSERT INTO t_ttl_merged_epoch_row VALUES ('1970-01-02 00:00:00');
INSERT INTO t_ttl_merged_epoch_row VALUES ('2100-01-01 00:00:00');

-- Change the TTL without materializing it, so the parts' stored TTL infos lag the metadata: a merge
-- that recalculates the TTL forcefully would now drop the epoch row (whose TTL under the new
-- expression is '1970-01-01 01:00:00', long expired) and stamp freshly computed bounds, while the
-- propagation path keeps both rows and the stored bounds. The epoch-only part must count as
-- "TTL calculated" (it has `has_epoch_timestamps`), or the merge takes the forced recalculation.
ALTER TABLE t_ttl_merged_epoch_row MODIFY TTL d - INTERVAL 23 HOUR SETTINGS materialize_ttl_after_modify = 0;

SYSTEM START MERGES t_ttl_merged_epoch_row;
OPTIMIZE TABLE t_ttl_merged_epoch_row FINAL;

-- Both rows survived, and the merged part's bounds are the propagated ones (computed under the
-- original `d - INTERVAL 1 DAY`): the merge did not take the forced TTL-recalculation path.
SELECT d FROM t_ttl_merged_epoch_row ORDER BY d;
SELECT toTimeZone(delete_ttl_info_min, 'UTC'), toTimeZone(delete_ttl_info_max, 'UTC')
    FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_merged_epoch_row' AND active;

-- The merged part must not have inherited the live sibling's fingerprint: this `MODIFY TTL` is a
-- provable constant shift, but the epoch row is invisible in the merged bounds, so the fast path must
-- not engage. The full rewrite evaluates the current TTL row by row and drops the epoch row.
ALTER TABLE t_ttl_merged_epoch_row MODIFY TTL d - INTERVAL 22 HOUR;
SELECT d FROM t_ttl_merged_epoch_row;
DROP TABLE t_ttl_merged_epoch_row;

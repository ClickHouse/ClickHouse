-- An epoch-only part (every row's TTL computes to exactly 1970-01-01 00:00:00 UTC) has no representable
-- TTL bounds, so it records the `has_epoch_timestamps` marker instead. That marker must survive a reload
-- of the part from disk: `ttl.txt` must be written even though all the numeric bounds are zero. Without
-- it, the reloaded part looks TTL-uncalculated, and the next merge recalculates the TTL forcefully under
-- the current metadata instead of propagating the stored infos of its sources.

SET alter_sync = 2;

DROP TABLE IF EXISTS t_ttl_epoch_reload;
CREATE TABLE t_ttl_epoch_reload (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d - INTERVAL 1 DAY
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;
SYSTEM STOP MERGES t_ttl_epoch_reload;

-- The first part's only computed TTL is the epoch. The second part is fingerprinted normally.
INSERT INTO t_ttl_epoch_reload VALUES ('1970-01-02 00:00:00');
INSERT INTO t_ttl_epoch_reload VALUES ('2100-01-01 00:00:00');

-- Change the TTL without materializing it, so the parts' stored TTL infos lag the metadata: a merge that
-- recalculates the TTL forcefully would drop the epoch row (whose TTL under the new expression is
-- '1970-01-01 01:00:00', long expired) and stamp freshly computed bounds, while the propagation path
-- keeps both rows and the stored bounds.
ALTER TABLE t_ttl_epoch_reload MODIFY TTL d - INTERVAL 23 HOUR SETTINGS materialize_ttl_after_modify = 0;

-- Reload every part from disk: the in-memory TTL infos are discarded and re-read from `ttl.txt`.
DETACH TABLE t_ttl_epoch_reload;
ATTACH TABLE t_ttl_epoch_reload;

OPTIMIZE TABLE t_ttl_epoch_reload FINAL;

-- Both rows survived the merge, and the merged part's bounds are the propagated ones (computed under the
-- original `d - INTERVAL 1 DAY`): the reloaded epoch-only part still counted as "TTL calculated".
SELECT d FROM t_ttl_epoch_reload ORDER BY d;
SELECT toTimeZone(delete_ttl_info_min, 'UTC'), toTimeZone(delete_ttl_info_max, 'UTC')
    FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_epoch_reload' AND active;

-- The merged part must not have inherited the live sibling's fingerprint: the epoch row is invisible in
-- the merged bounds, so the metadata-only fast path must not engage; the full rewrite drops the epoch row.
ALTER TABLE t_ttl_epoch_reload MODIFY TTL d - INTERVAL 22 HOUR;
SELECT d FROM t_ttl_epoch_reload;
DROP TABLE t_ttl_epoch_reload;

-- A part with no rows describes no rows-TTL timestamps, so it must not destroy the rows-TTL
-- provenance of its live merge siblings. An empty part is created without a fingerprint by every
-- `createEmptyPart` caller - among them the fully expired branch of the fast `MATERIALIZE TTL` path
-- and the `TTLDrop` short circuit of a merge - and merging one with a live, fingerprinted part used
-- to clear the merged part's fingerprint, needlessly sending the next shiftable `MODIFY TTL` through
-- a full rewrite. Below, the first `MODIFY TTL` empties the only part, a live part is inserted, the
-- two are merged, and the second `MODIFY TTL` must still be a metadata-only shift that reads no rows.

SET alter_sync = 2;

DROP TABLE IF EXISTS t_ttl_merged_empty_part;
CREATE TABLE t_ttl_merged_empty_part (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d + INTERVAL 6311520000 SECOND
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0, remove_empty_parts = 0;

INSERT INTO t_ttl_merged_empty_part VALUES ('2020-01-01 00:00:00');
-- Provable constant shift that expires the whole part: it is replaced with an empty part.
ALTER TABLE t_ttl_merged_empty_part MODIFY TTL d + INTERVAL 1 SECOND;
SELECT count() FROM t_ttl_merged_empty_part;

-- A live part with a fingerprint of its own, merged with the empty one.
INSERT INTO t_ttl_merged_empty_part VALUES ('2100-01-01 00:00:00');
OPTIMIZE TABLE t_ttl_merged_empty_part FINAL;

-- The merged part still knows the expression its bounds were computed under, so this is a shift.
ALTER TABLE t_ttl_merged_empty_part MODIFY TTL d + INTERVAL 2 SECOND;
SELECT count() FROM t_ttl_merged_empty_part;

SYSTEM FLUSH LOGS part_log;
SELECT sum(read_rows) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_ttl_merged_empty_part' AND event_type = 'MutatePart';

DROP TABLE t_ttl_merged_empty_part;

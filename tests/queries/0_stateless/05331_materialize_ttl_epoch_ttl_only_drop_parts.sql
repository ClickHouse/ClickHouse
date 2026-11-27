-- A row whose rows TTL computes to exactly 1970-01-01 00:00:00 UTC means "no TTL" and is excluded from
-- the stored bounds, so a part mixing it with an already expired row stores bounds entirely in the past.
-- With `ttl_only_drop_parts = 1`, `MATERIALIZE TTL` is wrapped in `ExecutableTaskDropTTLExpiredPartsDecorator`,
-- which must not replace such a part with an empty one after rewriting it. Rows are not removed one by one
-- with `ttl_only_drop_parts = 1`, so both rows stay.

SET alter_sync = 2;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_epoch_only_drop_parts;
CREATE TABLE t_ttl_epoch_only_drop_parts (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d - INTERVAL 1 DAY
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0, ttl_only_drop_parts = 1;

-- Keep the part from being dropped by a TTL merge before the `ALTER`.
SYSTEM STOP TTL MERGES t_ttl_epoch_only_drop_parts;

-- The first row's TTL is exactly the epoch, the second one's is long expired.
INSERT INTO t_ttl_epoch_only_drop_parts VALUES ('1970-01-02 00:00:00'), ('2020-01-02 00:00:00');

ALTER TABLE t_ttl_epoch_only_drop_parts MATERIALIZE TTL;
SELECT d FROM t_ttl_epoch_only_drop_parts ORDER BY d;
DROP TABLE t_ttl_epoch_only_drop_parts;

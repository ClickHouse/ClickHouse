-- The TTL machinery treats a computed TTL timestamp of exactly 0 (the epoch, '1970-01-01 00:00:00' UTC)
-- as "no TTL": `ITTLAlgorithm::isTTLExpired` never expires such a row, and the part's stored TTL bounds
-- exclude it from `min`. The fast path of `MATERIALIZE TTL` proves its shift from those bounds, so it
-- must not trust them when an epoch timestamp is involved - in either direction.
-- `min_bytes_for_full_part_storage` is pinned so the part is stored with a file per column and the fast
-- path is eligible; `materialize_ttl_recalculate_only` is pinned so `MODIFY TTL` really rewrites parts.

SET alter_sync = 2;

SELECT 'A row whose TTL timestamp is exactly the epoch must still be dropped by MODIFY TTL';
-- The epoch row is invisible in the part's stored TTL bounds, so a blind shift would conclude that no
-- row is expired and keep both rows, while a full rewrite computes the epoch row's new timestamp
-- (now non-zero and long expired) and deletes it.
DROP TABLE IF EXISTS t_ttl_epoch_row;
CREATE TABLE t_ttl_epoch_row (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d - INTERVAL 1 DAY
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;
SYSTEM STOP TTL MERGES t_ttl_epoch_row;
INSERT INTO t_ttl_epoch_row VALUES ('1970-01-02 00:00:00'), ('2100-01-01 00:00:00');
SELECT count() FROM t_ttl_epoch_row;
ALTER TABLE t_ttl_epoch_row MODIFY TTL d - INTERVAL 23 HOUR;
SELECT d FROM t_ttl_epoch_row;
DROP TABLE t_ttl_epoch_row;

SELECT 'A row whose TTL timestamp shifts onto the epoch must not be dropped by MODIFY TTL';
-- The shift moves the row's timestamp from '2100-01-01 00:00:00' (not expired, so the full rewrite
-- examines the rows one by one) to exactly 0, which means "never expires": the full rewrite keeps the
-- row, so the fast path must not conclude from the shifted bounds that the whole part is expired and
-- replace it with an empty one.
SET allow_suspicious_ttl_expressions = 1;
DROP TABLE IF EXISTS t_ttl_onto_epoch;
CREATE TABLE t_ttl_onto_epoch (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;
SYSTEM STOP TTL MERGES t_ttl_onto_epoch;
INSERT INTO t_ttl_onto_epoch VALUES ('2100-01-01 00:00:00');
SELECT count() FROM t_ttl_onto_epoch;
ALTER TABLE t_ttl_onto_epoch MODIFY TTL d - INTERVAL 4102444800 SECOND;
SELECT d FROM t_ttl_onto_epoch;
DROP TABLE t_ttl_onto_epoch;

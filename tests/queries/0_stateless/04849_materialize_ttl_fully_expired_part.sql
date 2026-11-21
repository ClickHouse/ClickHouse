-- When a part's stored max TTL is already expired, the regular `MATERIALIZE TTL` rewrite drops the
-- part's data wholesale before even evaluating the new TTL expression (`all_data_dropped` in
-- `TTLTransform` is decided on the part's stored TTL infos). The fast shift path of `MATERIALIZE TTL`
-- must produce the identical result, so it must not clone-and-keep such a part even when every row is
-- live under the new, extended TTL. Both statements below extend the TTL of a fully expired part by
-- 200 years; the first is not provable as a constant shift (calendar interval) and takes the regular
-- rewrite, the second is provable (second interval) and is eligible for the fast path - the surviving
-- row counts must match.

SET alter_sync = 2;

SELECT 'Regular rewrite: extending the TTL of a fully expired part';
DROP TABLE IF EXISTS t_ttl_expired_regular;
CREATE TABLE t_ttl_expired_regular (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d + INTERVAL 1 SECOND
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;
SYSTEM STOP TTL MERGES t_ttl_expired_regular;
INSERT INTO t_ttl_expired_regular VALUES ('2020-01-01 00:00:00');
SELECT count() FROM t_ttl_expired_regular;
ALTER TABLE t_ttl_expired_regular MODIFY TTL d + INTERVAL 200 YEAR;
SELECT count() FROM t_ttl_expired_regular;
DROP TABLE t_ttl_expired_regular;

SELECT 'Fast path: extending the TTL of a fully expired part';
DROP TABLE IF EXISTS t_ttl_expired_fast;
CREATE TABLE t_ttl_expired_fast (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d + INTERVAL 1 SECOND
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;
SYSTEM STOP TTL MERGES t_ttl_expired_fast;
INSERT INTO t_ttl_expired_fast VALUES ('2020-01-01 00:00:00');
SELECT count() FROM t_ttl_expired_fast;
ALTER TABLE t_ttl_expired_fast MODIFY TTL d + INTERVAL 6311520000 SECOND;
SELECT count() FROM t_ttl_expired_fast;
DROP TABLE t_ttl_expired_fast;

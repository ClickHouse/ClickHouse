-- The fast `MODIFY TTL` path proves a constant shift between the part's stored rows-TTL expression
-- and the new one, then applies it to the part's stored TTL bounds. With an absurdly large interval
-- in the new TTL the proven shift itself fits in `time_t` but applying it to the bounds overflows -
-- undefined behavior, found by UBSan in a stress run - and the wrapped result made the fast path
-- treat the part as fully expired and replace it with an empty part. Such a shift must be rejected:
-- the regular rewrite evaluates the new expression as is (it saturates), and the row survives.

SET alter_sync = 2;

DROP TABLE IF EXISTS t_ttl_shift_overflow;
CREATE TABLE t_ttl_shift_overflow (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d + INTERVAL 1 SECOND
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;

INSERT INTO t_ttl_shift_overflow VALUES ('2100-01-01 00:00:02');
SELECT count() FROM t_ttl_shift_overflow;

-- Both TTLs shift `d` by literal seconds, so the delta proof succeeds with a delta of
-- 9223372036854775799 - but adding it to the part's stored bound overflows `time_t`.
ALTER TABLE t_ttl_shift_overflow MODIFY TTL d + INTERVAL 9223372036854775800 SECOND;
SELECT d FROM t_ttl_shift_overflow;
DROP TABLE t_ttl_shift_overflow;

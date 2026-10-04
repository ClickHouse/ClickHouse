-- An experimental codec in `TTL ... RECOMPRESS` is gated by its `enable_<family>_codec` setting only.
-- `allow_suspicious_ttl_expressions` is an escape hatch for suspicious TTL expressions and must not
-- double as a way to introduce an experimental codec.

DROP TABLE IF EXISTS t_ttl_experimental_codec;

SET enable_zxc_codec = 0;
SET allow_suspicious_ttl_expressions = 0;

CREATE TABLE t_ttl_experimental_codec (d Date, x UInt64)
ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZXC); -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_ttl_expressions = 1;

-- Still rejected: the suspicious-expression escape hatch does not enable an experimental codec.
CREATE TABLE t_ttl_experimental_codec (d Date, x UInt64)
ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZXC); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_ttl_experimental_codec (d Date, x UInt64) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_ttl_experimental_codec MODIFY TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZXC); -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_ttl_expressions = 0;
SET enable_zxc_codec = 1;

-- Allowed once the codec switch is on.
ALTER TABLE t_ttl_experimental_codec MODIFY TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZXC);
INSERT INTO t_ttl_experimental_codec SELECT toDate('2020-01-01') + number % 10, number FROM numbers(1000);
SELECT 'rows', count(), sum(x) FROM t_ttl_experimental_codec;

DROP TABLE t_ttl_experimental_codec;

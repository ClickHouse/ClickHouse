-- https://github.com/ClickHouse/ClickHouse/issues/91415

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int) ENGINE = Memory;

INSERT INTO TABLE t0 (c0) VALUES (2), (3), (1);

-- Pin the codec to `LZ4` so the exact ratios do not depend on the server's default compression codec.
SELECT estimateCompressionRatio('LZ4')(c0) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t0 SETTINGS use_variant_as_common_type = 1;

DROP TABLE t0;

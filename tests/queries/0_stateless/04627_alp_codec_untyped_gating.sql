-- The ALP codec built without a column type falls back to the `Float64` element width, reinterprets
-- the bytes as floating-point values and throws for any input whose size is not a multiple of that
-- width, so it cannot reliably compress untyped data and must be rejected everywhere a codec is
-- resolved without a column type — even with `allow_experimental_codecs` enabled, because this is a
-- data-safety property, not the codec gate.

SET enable_alp_codec = 1;

-- The untyped MergeTree compression settings reject it, both directly and inside a chain.
DROP TABLE IF EXISTS t_alp_s;
CREATE TABLE t_alp_s (x Float64) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'ALP'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_alp_s (x Float64) ENGINE = MergeTree ORDER BY tuple() SETTINGS marks_compression_codec = 'ALP'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_alp_s (x Float64) ENGINE = MergeTree ORDER BY tuple() SETTINGS primary_key_compression_codec = 'ALP'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_alp_s (x Float64) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'ALP, ZSTD(1)'; -- { serverError BAD_ARGUMENTS }

-- TTL ... RECOMPRESS resolves the codec without a column type, so ALP is rejected there too.
DROP TABLE IF EXISTS t_alp_ttl;
CREATE TABLE t_alp_ttl (d Date, x Float64)
ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ALP); -- { serverError BAD_ARGUMENTS }

-- As a per-column codec on a floating-point column (the type is known) it keeps working.
DROP TABLE IF EXISTS t_alp;
CREATE TABLE t_alp (id UInt64, x Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alp SELECT number, number / 8 FROM numbers(1000);
SELECT 'column_codec', countIf(x != id / 8) FROM t_alp;
DROP TABLE t_alp;

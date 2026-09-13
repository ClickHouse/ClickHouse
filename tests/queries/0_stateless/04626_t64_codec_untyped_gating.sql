-- The T64 codec cannot compress without a column type (its compression stores the column type id in
-- the stream and throws when it is unknown), so it must be rejected everywhere a codec is resolved
-- without one, the same way as other type-dependent codecs.

-- The untyped MergeTree compression settings reject it, both directly and inside a chain.
DROP TABLE IF EXISTS t_t64_s;
CREATE TABLE t_t64_s (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'T64'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_t64_s (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS marks_compression_codec = 'T64'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_t64_s (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS primary_key_compression_codec = 'T64'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_t64_s (x UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'T64, ZSTD(1)'; -- { serverError BAD_ARGUMENTS }

-- TTL ... RECOMPRESS resolves the codec without a column type, so T64 is rejected there too.
DROP TABLE IF EXISTS t_t64_ttl;
CREATE TABLE t_t64_ttl (d Date, x UInt32)
ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(T64); -- { serverError BAD_ARGUMENTS }

-- As a per-column codec (the type is known) it keeps working, including through a codec-only
-- ALTER MODIFY COLUMN that does not restate the type.
DROP TABLE IF EXISTS t_t64;
CREATE TABLE t_t64 (id UInt64, x UInt32 CODEC(T64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_t64 SELECT number, number * 7 FROM numbers(1000);
SELECT 'column_codec', countIf(x != id * 7) FROM t_t64;
ALTER TABLE t_t64 MODIFY COLUMN id CODEC(T64, LZ4);
INSERT INTO t_t64 SELECT number, number * 7 FROM numbers(1000, 1000);
SELECT 'after_alter', countIf(x != id * 7), count() FROM t_t64;
DROP TABLE t_t64;

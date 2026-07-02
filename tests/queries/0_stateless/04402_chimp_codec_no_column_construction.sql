-- Tags: no-fasttest
-- no-fasttest: the fast-test build has no SSL, so the Encrypted codec creator throws and
-- `SELECT * FROM system.codecs` (which builds every codec for its metadata) cannot run; this
-- matches 01222_system_codecs. The Chimp round trip itself is also covered by 00950_test_chimp_codec.

-- Regression test for the Chimp codec no-column construction path.
-- The codec builder must stay constructible without an explicit width or a column type, because
-- that path is used for method-byte decoding (CompressionCodecFactory::get(uint8_t)) and for codec
-- metadata (system.codecs). Otherwise data written with the Chimp method byte cannot be read back
-- and `SELECT * FROM system.codecs` throws.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS t_chimp_multiple;

-- `CODEC(Chimp, LZ4)` is stored as a CompressionCodecMultiple. On read it reconstructs each
-- sub-codec from its method byte through the no-column path, which previously threw.
CREATE TABLE t_chimp_multiple
(
    x Float64 CODEC(Chimp, LZ4),
    y Float32 CODEC(Chimp)
)
ENGINE = MergeTree ORDER BY tuple();

-- Integer-valued floats: exactly representable in both Float64 and Float32 (max sum 499500 < 2^24),
-- so the read-back aggregates are deterministic.
INSERT INTO t_chimp_multiple SELECT number, number FROM numbers(1000);

-- Force a fresh read from the part (no-column method-byte reconstruction).
SELECT count(), sum(x), sum(y) FROM t_chimp_multiple;

DROP TABLE t_chimp_multiple;

-- system.codecs must list Chimp without throwing (no-column metadata construction).
SELECT name, is_compression, is_experimental FROM system.codecs WHERE name = 'Chimp';

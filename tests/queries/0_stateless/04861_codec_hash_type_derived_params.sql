-- A Compact part keys its shared CompressedStream on the codec hash, so a codec parameter derived
-- from the column type has to be part of that hash. The defect needs a Compact part, so the part
-- type is pinned on every table below instead of using a no-random-merge-tree-settings tag.

SET enable_alp_codec = 1; -- required by ALP (beta), not needed for FPC and GCD

-- 1. A Float64 and a Float32 column under the same codec name used to share one stream, so the
-- second column was compressed with the first one's float width and a misaligned granule was
-- rejected outright.

DROP TABLE IF EXISTS alp_reject;
CREATE TABLE alp_reject (n UInt64, f64 Float64 CODEC(ALP), f32 Float32 CODEC(ALP))
    ENGINE = MergeTree ORDER BY n
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000;
INSERT INTO alp_reject SELECT number, sin(number) * 1000, sin(number) * 1000 FROM numbers(101);
SELECT count(), uniqExact(part_type) = 1, any(part_type) FROM system.parts
    WHERE database = currentDatabase() AND table = 'alp_reject' AND active;
SELECT count() FROM alp_reject;

-- 2. Mixed-width columns must now compress to exactly the same bytes as two single-column tables.

DROP TABLE IF EXISTS alp_mix;
DROP TABLE IF EXISTS alp_32;
DROP TABLE IF EXISTS alp_64;
CREATE TABLE alp_mix (f32 Float32 CODEC(ALP), f64 Float64 CODEC(ALP))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
CREATE TABLE alp_32 (f32 Float32 CODEC(ALP))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
CREATE TABLE alp_64 (f64 Float64 CODEC(ALP))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
INSERT INTO alp_mix SELECT sin(number) * 1000, sin(number) * 1000 FROM numbers(20000);
INSERT INTO alp_32 SELECT sin(number) * 1000 FROM numbers(20000);
INSERT INTO alp_64 SELECT sin(number) * 1000 FROM numbers(20000);
SELECT (SELECT sum(data_compressed_bytes) FROM system.parts
            WHERE database = currentDatabase() AND table = 'alp_mix' AND active)
     = (SELECT sum(data_compressed_bytes) FROM system.parts
            WHERE database = currentDatabase() AND table IN ('alp_32', 'alp_64') AND active);

-- The declaration order used to change the result, because whichever column lost the race got
-- the other one's width.
DROP TABLE IF EXISTS alp_rev;
CREATE TABLE alp_rev (f64 Float64 CODEC(ALP), f32 Float32 CODEC(ALP))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
INSERT INTO alp_rev SELECT sin(number) * 1000, sin(number) * 1000 FROM numbers(20000);
SELECT (SELECT sum(data_compressed_bytes) FROM system.parts
            WHERE database = currentDatabase() AND table = 'alp_rev' AND active)
     = (SELECT sum(data_compressed_bytes) FROM system.parts
            WHERE database = currentDatabase() AND table = 'alp_mix' AND active);

-- 3. FPC has the same defect and is not gated.

DROP TABLE IF EXISTS fpc_mix;
DROP TABLE IF EXISTS fpc_32;
DROP TABLE IF EXISTS fpc_64;
CREATE TABLE fpc_mix (f32 Float32 CODEC(FPC), f64 Float64 CODEC(FPC))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
CREATE TABLE fpc_32 (f32 Float32 CODEC(FPC))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
CREATE TABLE fpc_64 (f64 Float64 CODEC(FPC))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
INSERT INTO fpc_mix SELECT sin(number) * 1000, sin(number) * 1000 FROM numbers(20000);
INSERT INTO fpc_32 SELECT sin(number) * 1000 FROM numbers(20000);
INSERT INTO fpc_64 SELECT sin(number) * 1000 FROM numbers(20000);
SELECT (SELECT sum(data_compressed_bytes) FROM system.parts
            WHERE database = currentDatabase() AND table = 'fpc_mix' AND active)
     = (SELECT sum(data_compressed_bytes) FROM system.parts
            WHERE database = currentDatabase() AND table IN ('fpc_32', 'fpc_64') AND active);

-- 4. A mixed-width Tuple is a second way to reach the rejection, because the codec is resolved
-- per substream.

DROP TABLE IF EXISTS alp_tuple;
CREATE TABLE alp_tuple (n UInt64, t Tuple(Float64, Float32) CODEC(ALP))
    ENGINE = MergeTree ORDER BY n
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000;
INSERT INTO alp_tuple SELECT number, (sin(number) * 1000, sin(number) * 1000) FROM numbers(1001);
SELECT count(), countIf(t.1 = toFloat64(toFloat32(t.2))) FROM alp_tuple;

-- Codec arguments are only substituted into the stored codec when every substream resolves to the
-- same codec, so a mixed-width tuple keeps FPC unresolved while a same-width one still resolves.
-- Delta, whose type-derived width was already hashed, has always behaved this way.
DROP TABLE IF EXISTS fpc_tuple_mixed;
DROP TABLE IF EXISTS fpc_tuple_same;
DROP TABLE IF EXISTS delta_tuple_mixed;
CREATE TABLE fpc_tuple_mixed (t Tuple(Float32, Float64) CODEC(FPC, LZ4))
    ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE fpc_tuple_same (t Tuple(Float32, Float32) CODEC(FPC, LZ4))
    ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE delta_tuple_mixed (t Tuple(UInt32, UInt64) CODEC(Delta, LZ4))
    ENGINE = MergeTree ORDER BY tuple();
SELECT name, extract(create_table_query, 'CODEC\\(.*?\\)\\)') FROM system.tables
    WHERE database = currentDatabase() AND name LIKE '%_tuple_%' ORDER BY name;

-- 5. GCD reaches the same collision through `gcd_bytes_size`, which is also type-derived. Whichever
-- column is declared first fixes the divisor width for the other one, so the result depends on the
-- declaration order until the width is hashed.

DROP TABLE IF EXISTS gcd_mix;
DROP TABLE IF EXISTS gcd_rev;
DROP TABLE IF EXISTS gcd_16;
DROP TABLE IF EXISTS gcd_64;
CREATE TABLE gcd_mix (u16 UInt16 CODEC(GCD, LZ4), u64 UInt64 CODEC(GCD, LZ4))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
CREATE TABLE gcd_rev (u64 UInt64 CODEC(GCD, LZ4), u16 UInt16 CODEC(GCD, LZ4))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
CREATE TABLE gcd_16 (u16 UInt16 CODEC(GCD, LZ4))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
CREATE TABLE gcd_64 (u64 UInt64 CODEC(GCD, LZ4))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000,
             index_granularity = 8192, index_granularity_bytes = 10485760;
INSERT INTO gcd_mix SELECT (number % 100) * 11, number * 1099511627776 FROM numbers(20000);
INSERT INTO gcd_rev SELECT number * 1099511627776, (number % 100) * 11 FROM numbers(20000);
INSERT INTO gcd_16 SELECT (number % 100) * 11 FROM numbers(20000);
INSERT INTO gcd_64 SELECT number * 1099511627776 FROM numbers(20000);
SELECT (SELECT sum(data_compressed_bytes) FROM system.parts
            WHERE database = currentDatabase() AND table = 'gcd_mix' AND active)
     = (SELECT sum(data_compressed_bytes) FROM system.parts
            WHERE database = currentDatabase() AND table IN ('gcd_16', 'gcd_64') AND active);
SELECT (SELECT sum(data_compressed_bytes) FROM system.parts
            WHERE database = currentDatabase() AND table = 'gcd_rev' AND active)
     = (SELECT sum(data_compressed_bytes) FROM system.parts
            WHERE database = currentDatabase() AND table = 'gcd_mix' AND active);
SELECT count(), countIf(u16 = (rn % 100) * 11 AND u64 = rn * 1099511627776)
    FROM (SELECT *, rowNumberInAllBlocks() AS rn FROM gcd_mix);

DROP TABLE alp_reject;
DROP TABLE alp_mix;
DROP TABLE alp_32;
DROP TABLE alp_64;
DROP TABLE alp_rev;
DROP TABLE fpc_mix;
DROP TABLE fpc_32;
DROP TABLE fpc_64;
DROP TABLE alp_tuple;
DROP TABLE fpc_tuple_mixed;
DROP TABLE fpc_tuple_same;
DROP TABLE delta_tuple_mixed;
DROP TABLE gcd_mix;
DROP TABLE gcd_rev;
DROP TABLE gcd_16;
DROP TABLE gcd_64;

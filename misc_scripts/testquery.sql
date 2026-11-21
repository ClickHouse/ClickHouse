/*

  ../build_release/programs/clickhouse-client \
  --queries-file ./testquery.sql \
  --echo \
  --progress \
  --time \
  > output.log


*/

SET allow_frame_of_reference_in_t64 = 1;

DROP TABLE IF EXISTS t64_source;
DROP TABLE IF EXISTS t64_bit_true_zstd;
DROP TABLE IF EXISTS t64_bit_false_zstd;
DROP TABLE IF EXISTS t64_byte_true_zstd;
DROP TABLE IF EXISTS t64_byte_false_zstd;
DROP TABLE IF EXISTS t64_bit_true_lz4;
DROP TABLE IF EXISTS t64_bit_false_lz4;
DROP TABLE IF EXISTS t64_byte_true_lz4;
DROP TABLE IF EXISTS t64_byte_false_lz4;



-- ============================================================
-- SCHEMA
-- ============================================================

CREATE TABLE t64_source
(
    n              UInt64,
    bit_boundaries Int64,
    sign_flipping  Int64,
    mid_range      Int64,
    small_range    Int64,
    random_noise   Int64,
    ts             Int64
)
ENGINE = MergeTree()
ORDER BY n;

CREATE TABLE t64_bit_true_zstd
(
    n              UInt64,
    bit_boundaries Int64 CODEC(T64('bit', true), ZSTD(1)),
    sign_flipping  Int64 CODEC(T64('bit', true), ZSTD(1)),
    mid_range      Int64 CODEC(T64('bit', true), ZSTD(1)),
    small_range    Int64 CODEC(T64('bit', true), ZSTD(1)),
    random_noise   Int64 CODEC(T64('bit', true), ZSTD(1)),
    ts             Int64 CODEC(T64('bit', true), ZSTD(1))
)
ENGINE = MergeTree() ORDER BY n;

CREATE TABLE t64_bit_false_zstd
(
    n              UInt64,
    bit_boundaries Int64 CODEC(T64('bit', false), ZSTD(1)),
    sign_flipping  Int64 CODEC(T64('bit', false), ZSTD(1)),
    mid_range      Int64 CODEC(T64('bit', false), ZSTD(1)),
    small_range    Int64 CODEC(T64('bit', false), ZSTD(1)),
    random_noise   Int64 CODEC(T64('bit', false), ZSTD(1)),
    ts             Int64 CODEC(T64('bit', false), ZSTD(1))
)
ENGINE = MergeTree() ORDER BY n;

CREATE TABLE t64_byte_true_zstd
(
    n              UInt64,
    bit_boundaries Int64 CODEC(T64('byte', true), ZSTD(1)),
    sign_flipping  Int64 CODEC(T64('byte', true), ZSTD(1)),
    mid_range      Int64 CODEC(T64('byte', true), ZSTD(1)),
    small_range    Int64 CODEC(T64('byte', true), ZSTD(1)),
    random_noise   Int64 CODEC(T64('byte', true), ZSTD(1)),
    ts             Int64 CODEC(T64('byte', true), ZSTD(1))
)
ENGINE = MergeTree() ORDER BY n;

CREATE TABLE t64_byte_false_zstd
(
    n              UInt64,
    bit_boundaries Int64 CODEC(T64('byte', false), ZSTD(1)),
    sign_flipping  Int64 CODEC(T64('byte', false), ZSTD(1)),
    mid_range      Int64 CODEC(T64('byte', false), ZSTD(1)),
    small_range    Int64 CODEC(T64('byte', false), ZSTD(1)),
    random_noise   Int64 CODEC(T64('byte', false), ZSTD(1)),
    ts             Int64 CODEC(T64('byte', false), ZSTD(1))
)
ENGINE = MergeTree() ORDER BY n;

CREATE TABLE t64_bit_true_lz4
(
    n              UInt64,
    bit_boundaries Int64 CODEC(T64('bit', true), LZ4),
    sign_flipping  Int64 CODEC(T64('bit', true), LZ4),
    mid_range      Int64 CODEC(T64('bit', true), LZ4),
    small_range    Int64 CODEC(T64('bit', true), LZ4),
    random_noise   Int64 CODEC(T64('bit', true), LZ4),
    ts             Int64 CODEC(T64('bit', true), LZ4)
)
ENGINE = MergeTree() ORDER BY n;

CREATE TABLE t64_bit_false_lz4
(
    n              UInt64,
    bit_boundaries Int64 CODEC(T64('bit', false), LZ4),
    sign_flipping  Int64 CODEC(T64('bit', false), LZ4),
    mid_range      Int64 CODEC(T64('bit', false), LZ4),
    small_range    Int64 CODEC(T64('bit', false), LZ4),
    random_noise   Int64 CODEC(T64('bit', false), LZ4),
    ts             Int64 CODEC(T64('bit', false), LZ4)
)
ENGINE = MergeTree() ORDER BY n;

CREATE TABLE t64_byte_true_lz4
(
    n              UInt64,
    bit_boundaries Int64 CODEC(T64('byte', true), LZ4),
    sign_flipping  Int64 CODEC(T64('byte', true), LZ4),
    mid_range      Int64 CODEC(T64('byte', true), LZ4),
    small_range    Int64 CODEC(T64('byte', true), LZ4),
    random_noise   Int64 CODEC(T64('byte', true), LZ4),
    ts             Int64 CODEC(T64('byte', true), LZ4)
)
ENGINE = MergeTree() ORDER BY n;

CREATE TABLE t64_byte_false_lz4
(
    n              UInt64,
    bit_boundaries Int64 CODEC(T64('byte', false), LZ4),
    sign_flipping  Int64 CODEC(T64('byte', false), LZ4),
    mid_range      Int64 CODEC(T64('byte', false), LZ4),
    small_range    Int64 CODEC(T64('byte', false), LZ4),
    random_noise   Int64 CODEC(T64('byte', false), LZ4),
    ts             Int64 CODEC(T64('byte', false), LZ4)
)
ENGINE = MergeTree() ORDER BY n;

-- ============================================================
-- INSERT
-- ============================================================

INSERT INTO t64_source
SELECT
    number,
    toInt64(number % 2 = 0 ? 2147483647 : 2147483649),
    toInt64(number % 2 = 0 ? 1 : -1),
    toInt64(16000 + (number % 4001)),
    toInt64(number % 257),
    reinterpretAsInt64(reverse(reinterpretAsFixedString(generateUUIDv4()))),
    toInt64(1739760000000 + (number % 100000) + (rand() % 1000))
FROM numbers(100000000);

INSERT INTO t64_bit_true_zstd   SELECT * FROM t64_source;
INSERT INTO t64_bit_false_zstd  SELECT * FROM t64_source;
INSERT INTO t64_byte_true_zstd  SELECT * FROM t64_source;
INSERT INTO t64_byte_false_zstd SELECT * FROM t64_source;
INSERT INTO t64_bit_true_lz4    SELECT * FROM t64_source;
INSERT INTO t64_bit_false_lz4   SELECT * FROM t64_source;
INSERT INTO t64_byte_true_lz4   SELECT * FROM t64_source;
INSERT INTO t64_byte_false_lz4  SELECT * FROM t64_source;

-- ============================================================
-- OPTIMIZE
-- ============================================================

OPTIMIZE TABLE t64_bit_true_zstd   FINAL;
OPTIMIZE TABLE t64_bit_false_zstd  FINAL;
OPTIMIZE TABLE t64_byte_true_zstd  FINAL;
OPTIMIZE TABLE t64_byte_false_zstd FINAL;
OPTIMIZE TABLE t64_bit_true_lz4    FINAL;
OPTIMIZE TABLE t64_bit_false_lz4   FINAL;
OPTIMIZE TABLE t64_byte_true_lz4   FINAL;
OPTIMIZE TABLE t64_byte_false_lz4  FINAL;

-- ============================================================
-- COMPARE
-- ============================================================

SELECT
    multiIf(
        table IN ('t64_bit_true_zstd',  't64_bit_false_zstd'),  'ZSTD | bit',
        table IN ('t64_byte_true_zstd', 't64_byte_false_zstd'), 'ZSTD | byte',
        table IN ('t64_bit_true_lz4',   't64_bit_false_lz4'),   'LZ4  | bit',
        table IN ('t64_byte_true_lz4',  't64_byte_false_lz4'),  'LZ4  | byte',
        table
    )                                                                            AS codec_group,
    multiIf(
        name = 'bit_boundaries', 'bit_boundaries — n%2==0 ? 2³¹-1 : 2³¹+1',
        name = 'sign_flipping',  'sign_flipping  — n%2==0 ? 1 : -1',
        name = 'mid_range',      'mid_range      — 16000 + (n%4001)',
        name = 'small_range',    'small_range    — n % 257',
        name = 'random_noise',   'random_noise   — reinterpretAsInt64(uuid())',
        name = 'ts',             'ts             — 1739760000000 + (n%100000) + rand()%1000',
        name
    )                                                                            AS pattern,
    formatReadableSize(max(data_uncompressed_bytes))                             AS uncompressed,
    formatReadableSize(maxIf(data_compressed_bytes, table IN ('t64_bit_true_zstd',  't64_byte_true_zstd',  't64_bit_true_lz4',  't64_byte_true_lz4')))  AS `true (offset reduced)`,
    formatReadableSize(maxIf(data_compressed_bytes, table IN ('t64_bit_false_zstd', 't64_byte_false_zstd', 't64_bit_false_lz4', 't64_byte_false_lz4'))) AS `false (default)`,
    round(max(data_uncompressed_bytes) / maxIf(data_compressed_bytes, table IN ('t64_bit_true_zstd',  't64_byte_true_zstd',  't64_bit_true_lz4',  't64_byte_true_lz4')),  4) AS `ratio true`,
    round(max(data_uncompressed_bytes) / maxIf(data_compressed_bytes, table IN ('t64_bit_false_zstd', 't64_byte_false_zstd', 't64_bit_false_lz4', 't64_byte_false_lz4')), 4) AS `ratio false`,
    round(
        maxIf(data_compressed_bytes, table IN ('t64_bit_false_zstd', 't64_byte_false_zstd', 't64_bit_false_lz4', 't64_byte_false_lz4'))
        /
        maxIf(data_compressed_bytes, table IN ('t64_bit_true_zstd',  't64_byte_true_zstd',  't64_bit_true_lz4',  't64_byte_true_lz4'))
    , 4)                                                                         AS `true vs false (multiplier)`
FROM system.columns
WHERE database = currentDatabase()
  AND table IN (
      't64_bit_true_zstd',  't64_bit_false_zstd',
      't64_byte_true_zstd', 't64_byte_false_zstd',
      't64_bit_true_lz4',   't64_bit_false_lz4',
      't64_byte_true_lz4',  't64_byte_false_lz4'
  )
  AND name != 'n'
GROUP BY codec_group, name
ORDER BY
    indexOf(['ZSTD | bit', 'ZSTD | byte', 'LZ4  | bit', 'LZ4  | byte'], codec_group),
    indexOf(['bit_boundaries','sign_flipping','mid_range','small_range','random_noise','ts'], name);

SELECT
    table,
    count()                                         AS row_count,
    sum(bit_boundaries)                             AS sum_bit_boundaries,
    sum(sign_flipping)                              AS sum_sign_flipping,
    sum(mid_range)                                  AS sum_mid_range,
    sum(small_range)                                AS sum_small_range,
    sum(random_noise)                               AS sum_random_noise,
    sum(ts)                                         AS sum_ts,
    avg(bit_boundaries)                             AS avg_bit_boundaries,
    avg(sign_flipping)                              AS avg_sign_flipping,
    avg(mid_range)                                  AS avg_mid_range,
    avg(small_range)                                AS avg_small_range,
    avg(ts)                                         AS avg_ts,
    min(bit_boundaries)                             AS min_bit_boundaries,
    min(sign_flipping)                              AS min_sign_flipping,
    min(mid_range)                                  AS min_mid_range,
    min(small_range)                                AS min_small_range,
    min(ts)                                         AS min_ts,
    max(bit_boundaries)                             AS max_bit_boundaries,
    max(sign_flipping)                              AS max_sign_flipping,
    max(mid_range)                                  AS max_mid_range,
    max(small_range)                                AS max_small_range,
    max(ts)                                         AS max_ts
FROM (
    SELECT 't64_source'         AS table, * FROM t64_source
    UNION ALL
    SELECT 't64_bit_true_zstd'  AS table, * FROM t64_bit_true_zstd
    UNION ALL
    SELECT 't64_bit_false_zstd' AS table, * FROM t64_bit_false_zstd
    UNION ALL
    SELECT 't64_byte_true_zstd' AS table, * FROM t64_byte_true_zstd
    UNION ALL
    SELECT 't64_byte_false_zstd'AS table, * FROM t64_byte_false_zstd
    UNION ALL
    SELECT 't64_bit_true_lz4'   AS table, * FROM t64_bit_true_lz4
    UNION ALL
    SELECT 't64_bit_false_lz4'  AS table, * FROM t64_bit_false_lz4
    UNION ALL
    SELECT 't64_byte_true_lz4'  AS table, * FROM t64_byte_true_lz4
    UNION ALL
    SELECT 't64_byte_false_lz4' AS table, * FROM t64_byte_false_lz4
)
GROUP BY table
ORDER BY table;

-- DROP TABLE IF EXISTS t64_source;
-- DROP TABLE IF EXISTS t64_bit_true_zstd;
-- DROP TABLE IF EXISTS t64_bit_false_zstd;
-- DROP TABLE IF EXISTS t64_byte_true_zstd;
-- DROP TABLE IF EXISTS t64_byte_false_zstd;
-- DROP TABLE IF EXISTS t64_bit_true_lz4;
-- DROP TABLE IF EXISTS t64_bit_false_lz4;
-- DROP TABLE IF EXISTS t64_byte_true_lz4;
-- DROP TABLE IF EXISTS t64_byte_false_lz4;
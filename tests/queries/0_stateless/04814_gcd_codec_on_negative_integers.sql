SET optimize_trivial_insert_select = 0;

-------------------------------------------------------------------------------------------
-- Case 1: Verify that GCD compression works for signed integers and decimals with negative values.
-- GCD compression applies to any fixed-width integer storage type (1, 2, 4, 8, 16, 32 bytes).
-- This table deliberately uses values of ~10^15 (multiples of 10^15) so the GCD divisor is
-- large enough to produce a clearly measurable compression ratio.  Those values overflow
-- Int8/Int16/Int32, so the smaller signed widths are omitted here and covered by the
-- round-trip test (use case 2) below.
DROP TABLE IF EXISTS table_gcd_codec_on_negative_integers;

CREATE TABLE table_gcd_codec_on_negative_integers
(
    symbol LowCardinality(String),
    ts DateTime64(6, 'UTC') CODEC(Delta, ZSTD),

    /* Signed integers */
    col_int64_plain       Int64 CODEC(ZSTD),
    col_int64_gcd         Int64 CODEC(GCD, ZSTD),
    col_int64_abs_gcd     Int64 CODEC(GCD, ZSTD),

    col_int128_plain      Int128 CODEC(ZSTD),
    col_int128_gcd        Int128 CODEC(GCD, ZSTD),
    col_int128_abs_gcd    Int128 CODEC(GCD, ZSTD),

    col_int256_plain      Int256 CODEC(ZSTD),
    col_int256_gcd        Int256 CODEC(GCD, ZSTD),
    col_int256_abs_gcd    Int256 CODEC(GCD, ZSTD),

    /* Decimals (scale=3, safe for ~10^6 magnitude) */
    col_dec64_plain       Decimal64(3) CODEC(ZSTD),
    col_dec64_gcd         Decimal64(3) CODEC(GCD, ZSTD),
    col_dec64_abs_gcd     Decimal64(3) CODEC(GCD, ZSTD),

    col_dec128_plain      Decimal128(3) CODEC(ZSTD),
    col_dec128_gcd        Decimal128(3) CODEC(GCD, ZSTD),
    col_dec128_abs_gcd    Decimal128(3) CODEC(GCD, ZSTD),

    col_dec256_plain      Decimal256(3) CODEC(ZSTD),
    col_dec256_gcd        Decimal256(3) CODEC(GCD, ZSTD),
    col_dec256_abs_gcd    Decimal256(3) CODEC(GCD, ZSTD)
)
ENGINE = MergeTree
ORDER BY (symbol, ts)
/* Force wide parts: Compact parts store all columns in a single file and track only the
   whole-part size, so system.columns would report zero compressed/uncompressed bytes for
   every column and all the per-column ratios computed below would degenerate to 0. */
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO table_gcd_codec_on_negative_integers
SELECT
    'binance:spot:SOLUSDT',
    now64(6),

    /* SIGNED INTEGERS (~10^15) */
    x_big, x_big, abs(x_big),

    toInt128(x_big), toInt128(x_big), toInt128(abs(x_big)),
    toInt256(x_big), toInt256(x_big), toInt256(abs(x_big)),

    /* DECIMALS (~10^6, safe for Decimal64/128/256(3)) */
    toDecimal64(x_small, 3),  toDecimal64(x_small, 3), toDecimal64(abs(x_small), 3),

    toDecimal128(x_small, 3), toDecimal128(x_small, 3), toDecimal128(abs(x_small), 3),

    toDecimal256(x_small, 3), toDecimal256(x_small, 3), toDecimal256(abs(x_small), 3)
FROM
(
    SELECT
        /* Large magnitude for integer types. `intHash64` gives deterministic pseudo-random
           values, so a failure is reproducible. */
        (toInt64(intHash64(number) % 2001) - 1000) * 1000000000000000 AS x_big,
        /* Small magnitude for decimals */
        (toInt64(intHash64(number + 100000) % 2001) - 1000) * 1000 AS x_small
    FROM numbers(100000)
);

/* Compare GCD vs plain compression ratios for each type. */
WITH cols AS
(
    SELECT
        name,
        data_uncompressed_bytes,
        data_compressed_bytes,
        if(data_compressed_bytes = 0, 0.,
           data_uncompressed_bytes / data_compressed_bytes) AS ratio
        FROM system.columns
        WHERE `database` = currentDatabase()
            AND `table` = 'table_gcd_codec_on_negative_integers'
),
r AS
(
    SELECT
        /* Signed ints */
        maxIf(ratio, name='col_int64_plain')      AS r_int64_plain,
        maxIf(ratio, name='col_int64_gcd')        AS r_int64_gcd,
        maxIf(ratio, name='col_int64_abs_gcd')    AS r_int64_abs_gcd,

        maxIf(ratio, name='col_int128_plain')     AS r_int128_plain,
        maxIf(ratio, name='col_int128_gcd')       AS r_int128_gcd,
        maxIf(ratio, name='col_int128_abs_gcd')   AS r_int128_abs_gcd,

        maxIf(ratio, name='col_int256_plain')     AS r_int256_plain,
        maxIf(ratio, name='col_int256_gcd')       AS r_int256_gcd,
        maxIf(ratio, name='col_int256_abs_gcd')   AS r_int256_abs_gcd,

        /* Decimals */
        maxIf(ratio, name='col_dec64_plain')      AS r_dec64_plain,
        maxIf(ratio, name='col_dec64_gcd')        AS r_dec64_gcd,
        maxIf(ratio, name='col_dec64_abs_gcd')    AS r_dec64_abs_gcd,

        maxIf(ratio, name='col_dec128_plain')     AS r_dec128_plain,
        maxIf(ratio, name='col_dec128_gcd')       AS r_dec128_gcd,
        maxIf(ratio, name='col_dec128_abs_gcd')   AS r_dec128_abs_gcd,

        maxIf(ratio, name='col_dec256_plain')     AS r_dec256_plain,
        maxIf(ratio, name='col_dec256_gcd')       AS r_dec256_gcd,
        maxIf(ratio, name='col_dec256_abs_gcd')   AS r_dec256_abs_gcd
    FROM cols
)
SELECT
    /* MUST PASS as bug is fixed: signed GCD beats plain, and is within a reasonable factor of the abs-value GCD ratio. */
    (r_int64_gcd  > r_int64_plain)  AS int64_signed_gcd_better_than_plain,
    (r_int64_gcd  >= 0.5 * r_int64_abs_gcd) AS int64_signed_gcd_reasonable_vs_abs,

    (r_int128_gcd > r_int128_plain) AS int128_signed_gcd_better_than_plain,
    (r_int128_gcd >= 0.5 * r_int128_abs_gcd) AS int128_signed_gcd_reasonable_vs_abs,

    (r_int256_gcd > r_int256_plain) AS int256_signed_gcd_better_than_plain,
    (r_int256_gcd >= 0.5 * r_int256_abs_gcd) AS int256_signed_gcd_reasonable_vs_abs,

    (r_dec64_gcd  > r_dec64_plain)  AS dec64_signed_gcd_better_than_plain,
    (r_dec64_gcd  >= 0.5 * r_dec64_abs_gcd) AS dec64_signed_gcd_reasonable_vs_abs,

    (r_dec128_gcd > r_dec128_plain) AS dec128_signed_gcd_better_than_plain,
    (r_dec128_gcd >= 0.5 * r_dec128_abs_gcd) AS dec128_signed_gcd_reasonable_vs_abs,

    (r_dec256_gcd > r_dec256_plain) AS dec256_signed_gcd_better_than_plain,
    (r_dec256_gcd >= 0.5 * r_dec256_abs_gcd) AS dec256_signed_gcd_reasonable_vs_abs
FROM r;

DROP TABLE table_gcd_codec_on_negative_integers;

-------------------------------------------------------------------------------------------
-- Case 2: Verify that all signed integer widths round-trip correctly under GCD compression.
-- Each column uses INT_MIN for that width so the magnitude 2^(N-1) is unrepresentable
-- in the signed type — the exact edge case that [expr.unary.op] makes UB.  This includes
-- Int128/Int256, whose division takes a separate non-libdivide branch in compressDataForType.

DROP TABLE IF EXISTS table_gcd_codec_signed_min_roundtrip;
CREATE TABLE table_gcd_codec_signed_min_roundtrip
(
    a Int8   CODEC(GCD, ZSTD),
    b Int16  CODEC(GCD, ZSTD),
    c Int32  CODEC(GCD, ZSTD),
    d Int64  CODEC(GCD, ZSTD),
    e Int128 CODEC(GCD, ZSTD),
    f Int256 CODEC(GCD, ZSTD)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO table_gcd_codec_signed_min_roundtrip VALUES
    (-128,  -32768,       -2147483648,  -9223372036854775808,  toInt128('-170141183460469231731687303715884105728'),  toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968')),
    (-64,   -16384,       -1073741824,  -4611686018427387904,  toInt128(-100),  toInt256(-100)),
    ( 64,    16384,        1073741824,   4611686018427387904,  toInt128( 100),  toInt256( 100));

/* Compare the full row set (not just extrema) so a corrupted middle row cannot hide behind
   unchanged min/max values. */
SELECT a, b, c, d, e, f
FROM table_gcd_codec_signed_min_roundtrip
ORDER BY a;

DROP TABLE table_gcd_codec_signed_min_roundtrip;

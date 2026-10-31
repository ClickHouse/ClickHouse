#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_nan_XXXXXX.sqlite")
LC_DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_lc_float_XXXXXX.sqlite")
trap 'rm -f "$DB" "$LC_DB"' EXIT

${CLICKHOUSE_LOCAL} --query "
    SELECT
        CAST(nan, 'Float32') AS f32,
        CAST(nan, 'Float64') AS f64,
        toNullable(CAST(nan, 'Float64')) AS nullable_f64,
        CAST(inf, 'Float32') AS f32_inf,
        CAST(-inf, 'Float32') AS f32_neg_inf,
        CAST(inf, 'Float64') AS f64_inf,
        CAST(-inf, 'Float64') AS f64_neg_inf,
        CAST(1.0000000000000002, 'Float64') AS exact_f64
    FORMAT SQLite" > "$DB"

echo "SQLite format NaN roundtrip"
${CLICKHOUSE_LOCAL} \
    --input-format SQLite \
    --output-format TSV \
    --query "
        SELECT
            isNaN(f32), toTypeName(f32),
            isNaN(f64), toTypeName(f64),
            isNull(nullable_f64), isNaN(assumeNotNull(nullable_f64)), toTypeName(nullable_f64),
            isInfinite(f32_inf), f32_inf > 0, isInfinite(f32_neg_inf), f32_neg_inf < 0, toTypeName(f32_inf),
            isInfinite(f64_inf), f64_inf > 0, isInfinite(f64_neg_inf), f64_neg_inf < 0, toTypeName(f64_inf),
            reinterpretAsUInt64(exact_f64), toTypeName(exact_f64)
        FROM table" < "$DB"

echo "SQLite engine native NaN roundtrip"
${CLICKHOUSE_LOCAL} --multiquery --query "
    CREATE TABLE sqlite_nan
    (
        f32 Float32,
        f64 Float64,
        nullable_f64 Nullable(Float64),
        f32_inf Float32,
        f32_neg_inf Float32,
        f64_inf Float64,
        f64_neg_inf Float64,
        exact_f64 Float64
    )
    ENGINE = SQLite('$DB', 'table');

    SELECT
        isNaN(f32), toTypeName(f32),
        isNaN(f64), toTypeName(f64),
        isNull(nullable_f64), isNaN(assumeNotNull(nullable_f64)), toTypeName(nullable_f64),
        isInfinite(f32_inf), f32_inf > 0, isInfinite(f32_neg_inf), f32_neg_inf < 0, toTypeName(f32_inf),
        isInfinite(f64_inf), f64_inf > 0, isInfinite(f64_neg_inf), f64_neg_inf < 0, toTypeName(f64_inf),
        reinterpretAsUInt64(exact_f64), toTypeName(exact_f64)
    FROM sqlite_nan"

${CLICKHOUSE_LOCAL} --allow_suspicious_low_cardinality_types=1 --query "
    SELECT
        CAST(1.0000000000000002, 'LowCardinality(Float64)') AS lc_f64,
        CAST(1.0000000000000002, 'LowCardinality(Nullable(Float64))') AS lc_nullable_f64,
        CAST(nan, 'LowCardinality(Float64)') AS lc_nan,
        CAST(nan, 'LowCardinality(Nullable(Float64))') AS lc_nullable_nan
    FORMAT SQLite" > "$LC_DB"

echo "SQLite format LowCardinality native Float64 roundtrip"
${CLICKHOUSE_LOCAL} \
    --allow_suspicious_low_cardinality_types=1 \
    --input-format SQLite \
    --output-format TSV \
    --structure "lc_f64 LowCardinality(Float64), lc_nullable_f64 LowCardinality(Nullable(Float64)), lc_nan LowCardinality(Float64), lc_nullable_nan LowCardinality(Nullable(Float64))" \
    --query "
        SELECT
            reinterpretAsUInt64(lc_f64), toTypeName(lc_f64),
            reinterpretAsUInt64(assumeNotNull(lc_nullable_f64)), toTypeName(lc_nullable_f64),
            isNaN(lc_nan), toTypeName(lc_nan),
            isNaN(assumeNotNull(lc_nullable_nan)), toTypeName(lc_nullable_nan)
        FROM table" < "$LC_DB"

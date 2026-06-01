#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_format_nan_XXXXXX.sqlite")
trap 'rm -f "$DB"' EXIT

${CLICKHOUSE_LOCAL} --query "
    SELECT
        CAST(nan, 'Float32') AS f32,
        CAST(nan, 'Float64') AS f64,
        toNullable(CAST(nan, 'Float64')) AS nullable_f64,
        CAST(inf, 'Float32') AS f32_inf,
        CAST(-inf, 'Float32') AS f32_neg_inf,
        CAST(inf, 'Float64') AS f64_inf,
        CAST(-inf, 'Float64') AS f64_neg_inf
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
            isInfinite(f64_inf), f64_inf > 0, isInfinite(f64_neg_inf), f64_neg_inf < 0, toTypeName(f64_inf)
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
        f64_neg_inf Float64
    )
    ENGINE = SQLite('$DB', 'table');

    SELECT
        isNaN(f32), toTypeName(f32),
        isNaN(f64), toTypeName(f64),
        isNull(nullable_f64), isNaN(assumeNotNull(nullable_f64)), toTypeName(nullable_f64),
        isInfinite(f32_inf), f32_inf > 0, isInfinite(f32_neg_inf), f32_neg_inf < 0, toTypeName(f32_inf),
        isInfinite(f64_inf), f64_inf > 0, isInfinite(f64_neg_inf), f64_neg_inf < 0, toTypeName(f64_inf)
    FROM sqlite_nan"

#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# ClickHouse types that have no Delta counterpart (so CREATE of a new table rejects them) can still be
# the declared type of a column of a table attached to an existing Delta table. The write path then casts
# each value to the Delta column type with `accurateCast`: the matrix below pins what is stored for every
# such source type and which values are rejected instead of silently changed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_source_types"
TABLE="${ROOT}/t"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

# Delta columns: a signed 64-bit integer, a string, a double and an array of strings.
mkdir -p "${TABLE}/_delta_log"
cat > "${TABLE}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-source-types","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"i\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"s\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"f\",\"type\":\"double\",\"nullable\":false,\"metadata\":{}},{\"name\":\"a\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":false,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
EOF

versions() {
    echo "versions: $(($(find "${TABLE}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1))"
}

# Each line: declared ClickHouse types | VALUES row. Prints the error code when the INSERT is rejected.
# (stdin closed: clickhouse-local would otherwise read the here-document as INSERT data)
run_matrix() {
    while IFS='|' read -r types values; do
        ${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --allow_experimental_variant_type=1 --allow_experimental_dynamic_type=1 --allow_experimental_json_type=1 --allow_suspicious_low_cardinality_types=1 --allow_suspicious_fixed_string_types=1 --query "
            CREATE TABLE t (${types}) ENGINE = DeltaLakeLocal('${TABLE}');
            INSERT INTO t VALUES (${values});
        " 2>&1 < /dev/null | grep -oE '\([A-Z_]+\)$' | head -1
    done
}

echo "-- integer sources wider than long: values that fit are stored, the rest are rejected"
run_matrix <<'EOF'
i UInt64, s String, f Float64, a Array(String)|9223372036854775807, 'UInt64 max long', 0, []
i UInt64, s String, f Float64, a Array(String)|9223372036854775808, 'UInt64 too large', 0, []
i Int128, s String, f Float64, a Array(String)|-9223372036854775808, 'Int128 min long', 0, []
i Int128, s String, f Float64, a Array(String)|170141183460469231731687303715884105727, 'Int128 too large', 0, []
i UInt256, s String, f Float64, a Array(String)|1, 'UInt256 small', 0, []
i UInt256, s String, f Float64, a Array(String)|9223372036854775808, 'UInt256 too large', 0, []
EOF
versions

echo "-- non-numeric sources into a string column are stored in their text form"
run_matrix <<'EOF'
i Int64, s UUID, f Float64, a Array(String)|1, '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 0, []
i Int64, s Enum8('alpha' = 1, 'beta' = 2), f Float64, a Array(String)|2, 'beta', 0, []
i Int64, s IPv4, f Float64, a Array(String)|3, '192.168.0.1', 0, []
i Int64, s IPv6, f Float64, a Array(String)|4, '2001:db8::1', 0, []
i Int64, s LowCardinality(String), f Float64, a Array(String)|5, 'low cardinality', 0, []
i Int64, s FixedString(4), f Float64, a Array(String)|6, 'ab', 0, []
i Int64, s Date, f Float64, a Array(String)|7, '2024-06-01', 0, []
i Int64, s DateTime64(3, 'UTC'), f Float64, a Array(String)|8, '2024-06-01 12:34:56.789', 0, []
i Int64, s Bool, f Float64, a Array(String)|9, true, 0, []
i Int64, s JSON, f Float64, a Array(String)|10, '{"k":1}', 0, []
EOF
versions

echo "-- Variant and Dynamic sources: a value of a compatible type is stored (text is parsed), other text is rejected"
run_matrix <<'EOF'
i Variant(Int64, String), s String, f Float64, a Array(String)|11, 'variant int', 0, []
i Variant(Int64, String), s String, f Float64, a Array(String)|'not a number', 'variant string', 0, []
i Dynamic, s String, f Float64, a Array(String)|12, 'dynamic int', 0, []
i Dynamic, s String, f Float64, a Array(String)|'12', 'dynamic numeric string', 0, []
i Dynamic, s String, f Float64, a Array(String)|'twelve', 'dynamic text', 0, []
EOF
versions

echo "-- Nullable sources into non-nullable columns: values are stored, NULL is rejected"
run_matrix <<'EOF'
i Nullable(Int64), s String, f Float64, a Array(String)|13, 'nullable with value', 0, []
i Nullable(Int64), s String, f Float64, a Array(String)|NULL, 'nullable null', 0, []
EOF
versions

echo "-- float sources: Float32 widens, a Decimal is converted, a numeric string is parsed"
run_matrix <<'EOF'
i Int64, s String, f Float32, a Array(String)|14, 'float32', 0.5, []
i Int64, s String, f Decimal(10, 3), a Array(String)|15, 'decimal', 1.125, []
i Int64, s String, f String, a Array(String)|16, 'string into double', '1.5', []
EOF
versions

echo "-- array sources: LowCardinality elements and numbers are stored as strings; a Map becomes an array of tuple strings"
run_matrix <<'EOF'
i Int64, s String, f Float64, a Array(LowCardinality(String))|17, 'array lc', 0, ['x', 'y']
i Int64, s String, f Float64, a Array(UInt8)|18, 'array uint8', 0, [1, 2]
i Int64, s String, f Float64, a Map(String, String)|19, 'map', 0, {'k': 'v'}
EOF
versions

echo "-- what a fresh reader sees"
${CLICKHOUSE_LOCAL} --query "SELECT i, s, f, a FROM deltaLakeLocal('${TABLE}') ORDER BY i, s"

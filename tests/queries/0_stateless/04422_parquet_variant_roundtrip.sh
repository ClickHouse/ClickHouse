#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Round-trip a Variant column through the Parquet `variant` logical type: a ClickHouse Variant
# column is encoded directly into a Parquet variant group (type-preserving, no JSON), then read
# back into the fixed carrier
#   Variant(Bool, Int64, Float64, Date32, DateTime64(6,'UTC'), DateTime64(9,'UTC'), UUID, String,
#           Array(String), Map(String, String)).
# Cross-engine interop (reading a variant file produced by Spark) is covered by the integration
# test tests/integration/test_parquet_variant_spark.

FILE="${CLICKHOUSE_TMP}/04422_variant.parquet"
rm -f "$FILE"

V="Variant(Int64, String, Float64, Array(Int64), Map(String, String), Date32, DateTime64(6, 'UTC'), UUID)"
${CLICKHOUSE_LOCAL} --query "
SET enable_variant_type = 1;
SET allow_suspicious_variant_types = 1;
INSERT INTO FUNCTION file('${FILE}', Parquet)
SELECT arrayJoin([
    (42::Int64)::${V},
    ('abc'::String)::${V},
    (3.5::Float64)::${V},
    ([1, 2, 3]::Array(Int64))::${V},
    (map('k', 'v')::Map(String, String))::${V},
    (toDate32('2020-01-02')::Date32)::${V},
    (toDateTime64('2020-01-02 03:04:05.678901', 6, 'UTC'))::${V},
    (toUUID('00112233-4455-6677-8899-aabbccddeeff'))::${V}
]) AS v;
"

${CLICKHOUSE_LOCAL} --query "SET enable_variant_type = 1; DESCRIBE TABLE file('${FILE}', Parquet);"
${CLICKHOUSE_LOCAL} --query "SET enable_variant_type = 1; SELECT v, variantType(v) AS type FROM file('${FILE}', Parquet);"
rm -f "$FILE"

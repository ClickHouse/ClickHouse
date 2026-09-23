#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# The Delta protocol defines add.modificationTime as milliseconds since the epoch, so a
# seconds value reads back as a 1970 timestamp in Spark, delta-rs and DESCRIBE DETAIL.
# https://github.com/ClickHouse/ClickHouse/issues/120523

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_DATABASE:?}_delta_modtime"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]}'

mkdir -p "${ROOT}/_delta_log"
cat > "${ROOT}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-modtime","format":{"provider":"parquet","options":{}},"schemaString":"${SCHEMA}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
EOF

${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}') SELECT number::Int64 AS id FROM numbers(5);
"

# The digit count keeps a unit regression self-diagnosing (10 seconds, 13 milliseconds,
# 16 microseconds); the window pins the value to the time the file was actually written.
${CLICKHOUSE_LOCAL} --query "
    WITH
        JSONExtractInt(line, 'add', 'modificationTime') AS mt,
        toInt64(toUnixTimestamp(now())) * 1000 AS now_ms
    SELECT
        'modificationTime digits: ' || toString(min(length(toString(mt)))),
        'modificationTime is the current time in milliseconds: ' || toString(min(mt BETWEEN now_ms - 3600000 AND now_ms + 60000))
    FROM file('${ROOT}/_delta_log/00000000000000000001.json', LineAsString)
    WHERE JSONHas(line, 'add')
    FORMAT TSVRaw
" | tr '\t' '\n'

${CLICKHOUSE_LOCAL} --query "
    SELECT 'rows read back: ' || toString(count()) FROM deltaLakeLocal('${ROOT}') FORMAT TSVRaw
"

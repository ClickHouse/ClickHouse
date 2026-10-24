#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Accurate cast on write (`delta_lake_accurate_write_cast`) over the Delta primitive types: values that
# do not fit are rejected with nothing committed, boundary values round-trip, plain cast wraps.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_cast"
TABLE="${ROOT}/t"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

mkdir -p "${TABLE}/_delta_log"
cat > "${TABLE}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-cast","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"b\",\"type\":\"byte\",\"nullable\":true,\"metadata\":{}},{\"name\":\"sh\",\"type\":\"short\",\"nullable\":true,\"metadata\":{}},{\"name\":\"i\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"l\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"f\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"d\",\"type\":\"decimal(9,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"dt\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"s\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"bo\",\"type\":\"boolean\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
EOF

# Wider ClickHouse types than the Delta schema, so the cast happens in the sink.
CREATE="CREATE TABLE t (b Int32, sh Int32, i Int64, l UInt64, f Float64, d Decimal(18, 2), dt Date32, ts DateTime64(9), s String, bo Int32) ENGINE = DeltaLakeLocal('${TABLE}')"

state() {
    echo "versions: $(find "${TABLE}/_delta_log" -name '*.json' | wc -l | tr -d ' '), data files: $(find "${TABLE}" -name '*.parquet' | wc -l | tr -d ' ')"
}

error_code() {
    grep -oE '\([A-Z_]+\)' | head -1
}

# name | values that must be rejected (stdin closed: clickhouse-local would read the here-document)
while IFS='|' read -r name values; do
    echo "-- ${name}: rejected, nothing committed"
    ${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
        ${CREATE};
        INSERT INTO t VALUES (${values});
    " 2>&1 < /dev/null | error_code
    state
done <<'EOF'
byte overflow (300)|300, 0, 0, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00', '', 0
byte negative overflow (-129)|-129, 0, 0, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00', '', 0
short overflow (70000)|0, 70000, 0, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00', '', 0
integer overflow (3000000000)|0, 0, 3000000000, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00', '', 0
long overflow (UInt64 max)|0, 0, 0, 18446744073709551615, 0, 0, '2020-01-01', '2020-01-01 00:00:00', '', 0
float overflow (1e39)|0, 0, 0, 0, 1e39, 0, '2020-01-01', '2020-01-01 00:00:00', '', 0
EOF

echo "-- boundary values that fit are committed and read back exactly"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    ${CREATE};
    INSERT INTO t VALUES (127, 32767, 2147483647, 9223372036854775807, 3.5, 1234567.89, '2020-01-01', '2020-01-01 00:00:00', 'text', 1);
    INSERT INTO t VALUES (-128, -32768, -2147483648, 0, -0.25, -1234567.89, '1970-01-01', '1970-01-01 00:00:00', '', 0);
    SELECT * FROM t ORDER BY b;
"
state

echo "-- sub-microsecond precision is truncated to the timestamp's microseconds (not rejected)"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    ${CREATE};
    INSERT INTO t VALUES (3, 0, 0, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00.123456789', 'ts', 0);
    SELECT ts FROM deltaLakeLocal('${TABLE}') WHERE b = 3;
"

echo "-- a numeric source into a string column is stringified, not rejected"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO TABLE FUNCTION deltaLakeLocal('${TABLE}', 'Parquet', 'b Int32, sh Int32, i Int64, l UInt64, f Float64, d Decimal(18, 2), dt Date32, ts DateTime64(9), s UInt32, bo Bool')
    VALUES (1, 1, 1, 1, 1, 1, '2020-01-01', '2020-01-01 00:00:00', 42, false);
    SELECT s, toTypeName(s) FROM deltaLakeLocal('${TABLE}') WHERE b = 1;
"

echo "-- a timestamp keeps its instant regardless of the source column's time zone"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO TABLE FUNCTION deltaLakeLocal('${TABLE}', 'Parquet', 'b Int32, sh Int32, i Int64, l UInt64, f Float64, d Decimal(18, 2), dt Date32, ts DateTime(\'Asia/Tokyo\'), s String, bo Bool')
    VALUES (2, 0, 0, 0, 0, 0, '2020-01-01', toDateTime('2024-06-01 09:00:00', 'Asia/Tokyo'), 'tz', false);
    SELECT toUnixTimestamp(ts) = toUnixTimestamp(toDateTime('2024-06-01 09:00:00', 'Asia/Tokyo')), toTimeZone(ts, 'UTC') FROM deltaLakeLocal('${TABLE}') WHERE b = 2;
"

echo "-- a boolean column from an Int32 source: 0 and 1 round-trip; any other value becomes true, with or"
echo "-- without the accurate cast (accurateCast to Bool accepts every value, so nothing is rejected here)"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    ${CREATE};
    INSERT INTO t VALUES (10, 0, 0, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00', 'bool', 2);
    INSERT INTO t VALUES (11, 0, 0, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00', 'bool', -1);
    SELECT b, bo, toTypeName(bo) FROM deltaLakeLocal('${TABLE}') WHERE s = 'bool' ORDER BY b;
"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --delta_lake_accurate_write_cast=0 --query "
    ${CREATE};
    INSERT INTO t VALUES (12, 0, 0, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00', 'bool plain', 2);
    SELECT b, bo FROM deltaLakeLocal('${TABLE}') WHERE s = 'bool plain';
"

echo "-- with the accurate cast off the plain cast wraps silently (300 -> 44 in a byte column)"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --delta_lake_accurate_write_cast=0 --query "
    ${CREATE};
    INSERT INTO t VALUES (300, 0, 0, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00', 'wrapped', 0);
    SELECT b, s FROM t WHERE s = 'wrapped';
"

echo "-- compatibility below 26.9 selects the plain cast as well"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --compatibility='26.8' --query "
    ${CREATE};
    INSERT INTO t VALUES (300, 0, 0, 0, 0, 0, '2020-01-01', '2020-01-01 00:00:00', 'compat', 0);
    SELECT b, s FROM t WHERE s = 'compat';
"
state

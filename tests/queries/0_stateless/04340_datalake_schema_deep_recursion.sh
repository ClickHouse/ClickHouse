#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-msan: the delta-kernel-rs library is not built with MSan, so the deltaLakeLocal table
# function is not registered there and the delta case below would fail with UNKNOWN_FUNCTION.
# Regression test: DeltaLake/Iceberg parse table-metadata schema JSON with Poco::JSON::Parser, which
# defaulted to unlimited depth (setDepth was a no-op), so a deeply nested schema overflowed the
# native stack inside Poco's recursive parser. The parsers now set a depth limit, so deep nesting is
# rejected with a clean JSON exception instead of crashing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR/dl/_delta_log"
trap 'rm -rf "$TMP_DIR"' EXIT

# Reads a query's combined output from stdin. If it contains $2, print a stable "<label>: <marker>"
# line; otherwise print the actual output so a CI failure shows what really happened instead of FAIL.
expect_contains() {
    local label="$1" marker="$2" out
    out=$(cat)
    if printf '%s\n' "$out" | grep -qF "$marker"; then
        echo "$label: $marker"
    else
        echo "$label: expected '$marker', got:"
        printf '%s\n' "$out" | head -3
    fi
}

# Minimal Delta table whose metaData.schemaString is a struct nested 2000 deep. The depth only needs
# to exceed the parser depth limit (1000) to be rejected; we keep it modest so the test stays cheap
# and does not stress memory (a much deeper schema used to overflow the native stack unpatched).
python3 - 2000 "$TMP_DIR/dl/_delta_log/00000000000000000000.json" <<'PYEOF'
import json, sys
N = int(sys.argv[1])
pre = '{"type":"struct","fields":[{"name":"f","type":'
suf = ',"nullable":false,"metadata":{}}]}'
schema = '{"type":"struct","fields":[{"name":"x","type":' + pre * N + '"integer"' + suf * N + ',"nullable":false,"metadata":{}}]}'
proto = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
meta = {"metaData": {"id": "deep", "format": {"provider": "parquet", "options": {}},
                     "schemaString": schema, "partitionColumns": [], "configuration": {},
                     "createdTime": 1700000000000}}
open(sys.argv[2], "w").write(json.dumps(proto) + "\n" + json.dumps(meta) + "\n")
PYEOF

# allow_experimental_delta_kernel_rs=0 selects the C++ metadata parser (the Rust kernel has its own
# recursion limit). The deep schema must be rejected with a JSON exception, not crash the process.
$CLICKHOUSE_LOCAL --query "DESCRIBE TABLE deltaLakeLocal('$TMP_DIR/dl') SETTINGS allow_experimental_delta_kernel_rs=0 FORMAT Null" 2>&1 \
    | expect_contains delta_depth JSONException

# Iceberg parses the whole metadata JSON with Poco before any field validation, so a deeply nested
# metadata file overflows the parser (reachable at default settings, no kernel involved). Must be
# rejected with a JSON exception.
mkdir -p "$TMP_DIR/ice/metadata"
python3 -c "
N = 2000
open('$TMP_DIR/ice/metadata/v1.metadata.json', 'wb').write(b'{' + b'\"a\":{' * N + b'\"x\":1' + b'}' * N + b'}')
open('$TMP_DIR/ice/metadata/version-hint.text', 'w').write('1')
"
$CLICKHOUSE_LOCAL --query "DESCRIBE TABLE icebergLocal('$TMP_DIR/ice') FORMAT Null" 2>&1 \
    | expect_contains iceberg_depth JSONException

# Paimon parses its schema file (schema/schema-N) with Poco before validation; a deeply nested
# schema must be rejected with a JSON exception, not crash (reachable at default settings).
mkdir -p "$TMP_DIR/pm/schema"
python3 -c "
N = 2000
open('$TMP_DIR/pm/schema/schema-0', 'wb').write(b'{' + b'\"a\":{' * N + b'\"x\":1' + b'}' * N + b'}')
"
$CLICKHOUSE_LOCAL --query "DESCRIBE TABLE paimonLocal('$TMP_DIR/pm') FORMAT Null" 2>&1 \
    | expect_contains paimon_depth JSONException

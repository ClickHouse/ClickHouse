#!/usr/bin/env bash
# Tags: no-fasttest
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

# Minimal Delta table whose metaData.schemaString is a struct nested 50000 deep (O(depth) flat
# string; far above the parser depth limit, deep enough to overflow the native stack unpatched).
python3 - 50000 "$TMP_DIR/dl/_delta_log/00000000000000000000.json" <<'PYEOF'
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
    | grep -qF 'JSONException' && echo 'delta_depth OK' || echo 'delta_depth FAIL'

# Iceberg parses the whole metadata JSON with Poco before any field validation, so a deeply nested
# metadata file overflows the parser (reachable at default settings, no kernel involved). Must be
# rejected with a JSON exception.
mkdir -p "$TMP_DIR/ice/metadata"
python3 -c "
N = 50000
open('$TMP_DIR/ice/metadata/v1.metadata.json', 'wb').write(b'{' + b'\"a\":{' * N + b'\"x\":1' + b'}' * N + b'}')
open('$TMP_DIR/ice/metadata/version-hint.text', 'w').write('1')
"
$CLICKHOUSE_LOCAL --query "DESCRIBE TABLE icebergLocal('$TMP_DIR/ice') FORMAT Null" 2>&1 \
    | grep -qF 'JSONException' && echo 'iceberg_depth OK' || echo 'iceberg_depth FAIL'

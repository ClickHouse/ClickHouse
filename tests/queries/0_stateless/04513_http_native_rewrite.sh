#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

query="SELECT toDateTime(1676369730, 'Asia/Shanghai') AS dt FORMAT Native"

raw1_path="${CLICKHOUSE_TMP}/04513_http_native_without_version.bin"
raw2_path="${CLICKHOUSE_TMP}/04513_http_native_with_version.bin"

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary "$query" > "$raw1_path"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&client_protocol_version=54337" --data-binary "$query" > "$raw2_path"

hex_prefix=$(python3 - <<'PY' "$raw1_path"
import sys
from pathlib import Path
data = Path(sys.argv[1]).read_bytes()
print(data.hex(" ", 2))
PY
)

type_name=$(python3 - <<'PY' "$raw2_path"
import sys
from pathlib import Path
data = Path(sys.argv[1]).read_bytes()
print(data[14:39].decode())
PY
)

[ "$hex_prefix" = "0101 0264 7408 4461 7465 5469 6d65 425f eb63" ]
[ "$type_name" = "DateTime('Asia/Shanghai')" ]

echo "http_native_timezone"
echo "$hex_prefix"
echo "$type_name"

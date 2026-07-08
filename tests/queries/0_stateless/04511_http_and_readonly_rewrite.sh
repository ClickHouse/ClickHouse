#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

err1=$($CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&query=CREATE%20TABLE%20xxx%20(a%20Date)%20ENGINE%20%3D%20MergeTree(a,%20a,%20256)" 2>&1 || true)
err2=$($CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&readonly=0&query=CREATE%20TABLE%20xxx%20(a%20Date)%20ENGINE%20%3D%20MergeTree(a,%20a,%20256)" 2>&1 || true)

grep -q "Cannot execute query in readonly mode" <<<"$err1"
grep -q "Cannot modify 'readonly' setting in readonly mode" <<<"$err2"

echo "readonly_http_get"
echo "OK"

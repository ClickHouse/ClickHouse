#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

query="CREATE TABLE xxx (a Date) ENGINE = MergeTree(a, a, 256)"

err1=$($CLICKHOUSE_CURL -sS -G --data-urlencode "query=${query}" "${CLICKHOUSE_URL}" 2>&1 || true)
err2=$($CLICKHOUSE_CURL -sS -G --data-urlencode "readonly=0" --data-urlencode "query=${query}" "${CLICKHOUSE_URL}" 2>&1 || true)

grep -q "Cannot execute query in readonly mode" <<<"$err1"
grep -q "Cannot modify 'readonly' setting in readonly mode" <<<"$err2"

echo "readonly_http_get"
echo "OK"

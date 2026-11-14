#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A foreign SQL dialect (e.g. `polyglot`) is sent to the server verbatim, so the client refuses to
# forward external insert data. The `clickhouse_json` dialect is NOT a foreign SQL dialect — it is
# another serialization of a ClickHouse AST that the client deserializes locally without rewriting
# any query text — so its INSERT data must keep being streamed by the client as usual.

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x"

JSON=$($CLICKHOUSE_CLIENT -q "SELECT parseQueryToJSON('INSERT INTO t FORMAT TSV') FORMAT TSVRaw")

printf '1\n2\n3\n' | $CLICKHOUSE_CLIENT --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON"

echo "--- piped stdin data is inserted (expect: 3 6) ---"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t"

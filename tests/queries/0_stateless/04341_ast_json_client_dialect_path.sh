#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cover the `clickhouse-client` JSON-AST path of `dialect = clickhouse_json` (the
# `ClientBase::parseQuery` branch with its bespoke balanced-object scanning and delimiter handling):
# build JSON with `parseQueryToJSON` and send it through `${CLICKHOUSE_CLIENT} --dialect clickhouse_json`.
# (04340 exercises the same code via `clickhouse-local`; this covers the networked client.)

JSON=$(${CLICKHOUSE_CLIENT} -q "SELECT parseQueryToJSON('SELECT 42 AS answer') FORMAT TSVRaw")

# 1. A single JSON AST executes through the client.
${CLICKHOUSE_CLIENT} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON"

# 2. Two JSON ASTs separated by `;` both execute in multiquery mode (the delimiter is accepted).
${CLICKHOUSE_CLIENT} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json --multiquery -q "$JSON ; $JSON"

# 3. Trailing non-delimiter text after the balanced JSON object is rejected as excessive input;
#    the prefix must NOT be executed (no `42` printed), only the error is reported. Assert BOTH that
#    the error is present and that the prefix result `42` is absent — a client regression that prints
#    `42` from the first object and only then reports the trailing error would otherwise still pass.
OUT=$(${CLICKHOUSE_CLIENT} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json --multiquery -q "$JSON garbage" 2>&1)
echo "$OUT" | grep -qm1 'SYNTAX_ERROR' && echo 'error_reported' || echo 'NO_ERROR'
echo "$OUT" | grep -qxF '42' && echo 'PREFIX_EXECUTED' || echo 'prefix_skipped'

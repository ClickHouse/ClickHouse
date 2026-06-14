#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In multiquery mode the `clickhouse_json` dialect scans for one balanced top-level JSON object and
# parses only that prefix. It must require the next significant token after the object to be a
# statement delimiter (`;`) or end of input, mirroring the SQL path's "excessive input" check.
# Otherwise `<valid json ast> garbage` would deserialize and execute the prefix and only fail on the
# trailing text in the next iteration.

JSON=$(${CLICKHOUSE_LOCAL} -q "SELECT parseQueryToJSON('SELECT 1') FORMAT TSVRaw")

# 1. A lone JSON AST object is accepted and executed.
${CLICKHOUSE_LOCAL} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json --multiquery -q "$JSON"

# 2. Two JSON AST objects separated by `;` both execute (the delimiter is accepted).
${CLICKHOUSE_LOCAL} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json --multiquery -q "$JSON ; $JSON"

# 3. Trailing non-delimiter text after a balanced JSON object is rejected as excessive input; the
#    prefix must NOT be executed (no `1` is printed), only the error is reported.
${CLICKHOUSE_LOCAL} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json --multiquery -q "$JSON garbage" 2>&1 | grep -om1 'SYNTAX_ERROR'

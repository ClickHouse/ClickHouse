#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In the `clickhouse_json` dialect, `clickhouse-local`'s `input()` initializer reparses the original query
# to detect the INSERT format. It must reparse with the dialect/gate the query was ACCEPTED with, captured
# before the query's own `SETTINGS` clause is applied during execution. Otherwise a JSON
# `INSERT ... FROM input(...) SETTINGS dialect = 'clickhouse'` would be reparsed as SQL (the JSON `{...}`
# fails `ParserQuery`), and `SETTINGS allow_experimental_json_ast_dialect = 0` would trip the disabled-gate
# check inside the initializer — both failing an otherwise valid query.

# 1. Embedded `SETTINGS dialect = 'clickhouse'`: the input() initializer must still reparse the body as JSON.
JSON_DIALECT=$(${CLICKHOUSE_LOCAL} -q "SELECT parseQueryToJSON('INSERT INTO FUNCTION null(''x UInt8'') SELECT * FROM input(''x UInt8'') SETTINGS dialect = ''clickhouse'' FORMAT TSV') FORMAT TSVRaw")
printf '1\n2\n3\n' | ${CLICKHOUSE_LOCAL} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON_DIALECT" 2>&1 && echo 'json_dialect_ok'

# 2. Embedded `SETTINGS allow_experimental_json_ast_dialect = 0`: the gate captured at parse time stays on.
JSON_GATE=$(${CLICKHOUSE_LOCAL} -q "SELECT parseQueryToJSON('INSERT INTO FUNCTION null(''x UInt8'') SELECT * FROM input(''x UInt8'') SETTINGS allow_experimental_json_ast_dialect = 0 FORMAT TSV') FORMAT TSVRaw")
printf '1\n2\n3\n' | ${CLICKHOUSE_LOCAL} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON_GATE" 2>&1 && echo 'json_gate_ok'

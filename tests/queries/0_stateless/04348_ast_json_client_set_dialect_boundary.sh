#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In the `clickhouse_json` dialect the client parses JSON locally, applies any `SET` it contains to its
# context, then sends the original JSON body to the server, which re-parses it using the session dialect.
# Parser-affecting settings from a JSON `SET` (`dialect`, `allow_experimental_json_ast_dialect`) must NOT
# change how the server parses that same JSON body — otherwise the server would try to parse the JSON as
# SQL (or with the JSON parser disabled) and reject an otherwise valid query. The client pins the outbound
# transport dialect to match the JSON body so these `SET`s only affect subsequent queries.

# 1. JSON `SET dialect = 'clickhouse'`: the body must still be parsed as JSON by the server (not as SQL).
SET_DIALECT=$(${CLICKHOUSE_CLIENT} -q "SELECT parseQueryToJSON('SET dialect = ''clickhouse''') FORMAT TSVRaw")
${CLICKHOUSE_CLIENT} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json -q "$SET_DIALECT" 2>&1 && echo 'set_dialect_ok'

# 2. JSON `SET allow_experimental_json_ast_dialect = 0`: the experimental gate must stay on for the body
#    that carries it (the server enforces the gate before parsing JSON).
SET_GATE=$(${CLICKHOUSE_CLIENT} -q "SELECT parseQueryToJSON('SET allow_experimental_json_ast_dialect = 0') FORMAT TSVRaw")
${CLICKHOUSE_CLIENT} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json -q "$SET_GATE" 2>&1 && echo 'set_gate_ok'

# 3. A plain JSON query still round-trips unchanged.
SELECT_JSON=$(${CLICKHOUSE_CLIENT} -q "SELECT parseQueryToJSON('SELECT 42') FORMAT TSVRaw")
${CLICKHOUSE_CLIENT} --allow_experimental_json_ast_dialect 1 --dialect clickhouse_json -q "$SELECT_JSON"

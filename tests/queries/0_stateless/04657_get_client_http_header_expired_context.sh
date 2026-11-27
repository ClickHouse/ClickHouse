#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `getClientHTTPHeader` used to look the query context up while executing, and threw the logical error
# `Context has expired` when the function was built while analyzing a subquery whose context was already
# gone by the time the expression actions ran. The query below is allowed to fail with an ordinary error;
# it must not report a logical error.

QUERY="SELECT (SELECT getClientHTTPHeader(* APPLY lambda(tuple(x), toString(x)) EXCEPT '[0-9]' REPLACE (true AS \`h\`)) GROUP BY ALL INTERSECT ALL SELECT getClientHTTPHeader(* APPLY lambda(tuple(x), toString(x)) EXCEPT '[0-9]' REPLACE (NULL AS \`h\`)) GROUP BY ALL QUALIFY 2147483646 LIMIT -2147483647) INTERSECT ALL SELECT getClientHTTPHeader(* APPLY lambda(tuple(x), toString(x)) EXCEPT '[0-9]' REPLACE (true AS \`h\`)) GROUP BY ALL"

OUTPUT=$($CLICKHOUSE_CLIENT --allow_get_client_http_header=1 --query "$QUERY" 2>&1 || true)

if [[ "$OUTPUT" == *"Context has expired"* ]]
then
    echo "$OUTPUT"
else
    echo "OK"
fi

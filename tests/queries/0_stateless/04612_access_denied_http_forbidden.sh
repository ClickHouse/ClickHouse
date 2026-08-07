#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR=$(mktemp -d "$CUR_DIR"/04612_XXXXXX)
trap 'rm -rf "$TMP_DIR"' EXIT

# A user without any grants gets ACCESS_DENIED (error code 497) when reading a table.
# Over HTTP this must be reported as 403 Forbidden, not 500 Internal Server Error.

user="user_04612_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH no_password"

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${user}:@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

${CLICKHOUSE_CURL} -s -D "$TMP_DIR/headers" -o /dev/null "${URL}" --data-binary "SELECT * FROM system.numbers LIMIT 1"

head -n 1 "$TMP_DIR/headers" | sed 's/\r$//'
grep -o "X-ClickHouse-Exception-Code: 497" "$TMP_DIR/headers" | head -n 1

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"

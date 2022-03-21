#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_02015_allowed"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_02015_denied"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS role_02015"

${CLICKHOUSE_CLIENT} -q "CREATE ROLE role_02015"
${CLICKHOUSE_CLIENT} -q "CREATE USER u_02015_allowed";
${CLICKHOUSE_CLIENT} -q "CREATE USER u_02015_denied";

${CLICKHOUSE_CLIENT} -q "REVOKE ALL ON *.* FROM u_02015_allowed"
${CLICKHOUSE_CLIENT} -q "REVOKE ALL ON *.* FROM u_02015_denied"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts (id UInt32, s String) ENGINE = MergeTree ORDER BY id"

${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON async_inserts TO role_02015"
${CLICKHOUSE_CLIENT} -q "GRANT role_02015 to u_02015_allowed"

${CLICKHOUSE_CURL} -sS "$url&user=u_02015_denied" \
    -d 'INSERT INTO async_inserts FORMAT JSONEachRow {"id": 1, "s": "a"} {"id": 2, "s": "b"}' \
    | grep -o "Not enough privileges"

${CLICKHOUSE_CURL} -sS "$url&user=u_02015_allowed" \
    -d 'INSERT INTO async_inserts FORMAT JSONEachRow {"id": 1, "s": "a"} {"id": 2, "s": "b"}'

${CLICKHOUSE_CLIENT} -q "SELECT * FROM async_inserts ORDER BY id"

${CLICKHOUSE_CLIENT} -q "DROP TABLE async_inserts"
${CLICKHOUSE_CLIENT} -q "DROP USER u_02015_allowed"
${CLICKHOUSE_CLIENT} -q "DROP USER u_02015_denied"
${CLICKHOUSE_CLIENT} -q "DROP ROLE role_02015"

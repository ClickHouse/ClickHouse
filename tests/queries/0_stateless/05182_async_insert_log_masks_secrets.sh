#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The insert goes over HTTP, so the `VALUES` data is parsed on the server (data kind `Parsed`).
# Port 1 refuses the connection at once, so the flush fails without any DNS lookup. The log element
# is written while parsing, so the failed flush does not hide a leak.
# The column name carries the database name: masking never touches it, so it identifies these rows.

url="http://etl_user:SUPER_SECRET_PASSWORD@127.0.0.1:1/ingest"

${CLICKHOUSE_CURL} -sS -X POST --data-binary \
    "INSERT INTO FUNCTION url('${url}', 'CSV', '${CLICKHOUSE_DATABASE} String') VALUES ('row1')" \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&wait_for_async_insert_timeout=30" > /dev/null 2>&1

# The log element is written after the HTTP response is sent, so wait for it.
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"

    count=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count()
        FROM system.asynchronous_insert_log
        WHERE position(query, '${CLICKHOUSE_DATABASE}') > 0")

    [ "$count" -gt 0 ] && break
    sleep 0.5
done

${CLICKHOUSE_CLIENT} -q "
    SELECT
        countIf(position(query, '[HIDDEN]') > 0) > 0 AS masked,
        countIf(position(query, 'SUPER_SECRET_PASSWORD') > 0) AS leaked
    FROM system.asynchronous_insert_log
    WHERE position(query, '${CLICKHOUSE_DATABASE}') > 0"

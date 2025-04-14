#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_mt_async_insert;
DROP TABLE IF EXISTS t_mt_sync_insert;

CREATE TABLE t_mt_async_insert (id UInt64, s String)
ENGINE = MergeTree ORDER BY id SETTINGS async_insert = 1;

CREATE TABLE t_mt_sync_insert (id UInt64, s String)
ENGINE = MergeTree ORDER BY id SETTINGS async_insert = 0;"

url="${CLICKHOUSE_URL}&async_insert=0&wait_for_async_insert=1"

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO t_mt_async_insert VALUES (1, 'aa'), (2, 'bb')"
${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO t_mt_sync_insert VALUES (1, 'aa'), (2, 'bb')"

${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM t_mt_async_insert;
SELECT count() FROM t_mt_sync_insert;

SYSTEM FLUSH LOGS;
SELECT tables[1], ProfileEvents['AsyncInsertQuery'] FROM system.query_log
WHERE
    type = 'QueryFinish' AND
    current_database = currentDatabase() AND
    query ILIKE 'INSERT INTO t_mt_%sync_insert%'
ORDER BY tables[1];

DROP TABLE IF EXISTS t_mt_async_insert;
DROP TABLE IF EXISTS t_mt_sync_insert;"

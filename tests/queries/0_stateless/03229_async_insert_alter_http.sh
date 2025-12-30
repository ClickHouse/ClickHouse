#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel because the test uses FLUSH ASYNC INSERT QUEUE

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_async_insert_alter;
    CREATE TABLE t_async_insert_alter (id Int64, v1 Int64) ENGINE = MergeTree ORDER BY id SETTINGS async_insert = 1;
"

url="${CLICKHOUSE_URL}&async_insert=1&async_insert_busy_timeout_max_ms=300000&async_insert_busy_timeout_min_ms=300000&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0"

# ADD COLUMN

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO t_async_insert_alter VALUES (42, 24)"

$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_async_insert_alter ADD COLUMN value2 Int64;

    SYSTEM FLUSH ASYNC INSERT QUEUE;
    SYSTEM FLUSH LOGS;

    SELECT * FROM t_async_insert_alter ORDER BY id;
"

# MODIFY COLUMN

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO t_async_insert_alter VALUES (43, 34, 55)"

$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_async_insert_alter MODIFY COLUMN value2 String;

    SYSTEM FLUSH ASYNC INSERT QUEUE;
    SYSTEM FLUSH LOGS;

    SELECT * FROM t_async_insert_alter ORDER BY id;
"

## DROP COLUMN

${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO t_async_insert_alter VALUES ('100', '200', '300')"

$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_async_insert_alter DROP COLUMN value2;

    SYSTEM FLUSH ASYNC INSERT QUEUE;
    SYSTEM FLUSH LOGS;

    SELECT * FROM t_async_insert_alter ORDER BY id;
    SELECT query, data_kind, status FROM system.asynchronous_insert_log WHERE database = currentDatabase() AND table = 't_async_insert_alter' ORDER BY event_time_microseconds;

    DROP TABLE t_async_insert_alter;
"

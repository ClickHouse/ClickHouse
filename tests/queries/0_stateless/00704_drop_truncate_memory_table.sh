#!/usr/bin/env bash
set -e

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery --query="
DROP TABLE IF EXISTS memory;
CREATE TABLE memory (x UInt64) ENGINE = Memory;

SET max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

INSERT INTO memory SELECT * FROM numbers(1000);"


# NOTE Most of the time this query can start before the table will be dropped.
# And TRUNCATE or DROP query will test for correct locking inside ClickHouse.
# But if the table will be dropped before query - just pass.
# It's Ok, because otherwise the test will depend on the race condition in the test itself.

${CLICKHOUSE_CLIENT} --multiquery --query="
SET max_threads = 1;
SELECT count() FROM memory WHERE NOT ignore(sleep(0.0001));" 2>&1 | grep -c -P '^1000$|^0$|Table .+? doesn.t exist' &

sleep 0.05;

${CLICKHOUSE_CLIENT} --multiquery --query="
TRUNCATE TABLE memory;
DROP TABLE memory;
"

wait

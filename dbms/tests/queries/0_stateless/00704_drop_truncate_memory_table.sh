#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery --query="
DROP TABLE IF EXISTS test.memory;
CREATE TABLE test.memory (x UInt64) ENGINE = Memory;

SET max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

INSERT INTO test.memory SELECT * FROM numbers(1000);"


${CLICKHOUSE_CLIENT} --multiquery --query="
SET max_threads = 1;
SELECT count() FROM test.memory WHERE NOT ignore(sleep(0.0001));" &

sleep 0.05;

${CLICKHOUSE_CLIENT} --multiquery --query="
TRUNCATE TABLE test.memory;
DROP TABLE test.memory;
"

wait

#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/79887
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t SYNC"

keeper_path="/s3queue_queue_$RANDOM"
cols=$($CLICKHOUSE_CLIENT -q "SELECT arrayStringConcat(arrayMap(i -> 'c' || toString(i) || ' Int8', range(200)), ', ')")
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (${cols}) ENGINE = S3Queue('http://localhost:11111/test/', 'user', 'password', CSV) SETTINGS mode = 'ordered', keeper_path = '${keeper_path}', after_processing = 'keep'"

# MODIFY SETTING must be rejected before Keeper/runtime state is changed.
$CLICKHOUSE_CLIENT --max_query_size=1024 -q "ALTER TABLE t MODIFY SETTING after_processing = 'delete'" 2>&1 | grep -o -F -m1 "QUERY_IS_TOO_LARGE"

# The Keeper metadata must still show after_processing = keep.
$CLICKHOUSE_CLIENT -q "SELECT JSONExtractString(value, 'after_processing') FROM system.zookeeper WHERE path = '${keeper_path}' AND name = 'metadata'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t SYNC"

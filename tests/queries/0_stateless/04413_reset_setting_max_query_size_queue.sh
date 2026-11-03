#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-shared-merge-tree, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/79887
# RESET SETTING removes the setting from the metadata, then re-adds it with its default value.
# The intermediate (reset-removed) metadata can be under max_query_size while the final
# (default re-added) metadata is over it. The rejected ALTER must not have changed Keeper state.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS q SYNC"

keeper_path="/q_reset_$RANDOM"
cols=$($CLICKHOUSE_CLIENT -q "SELECT arrayStringConcat(arrayMap(i -> 'c' || toString(i) || ' Int8', range(80)), ', ')")
$CLICKHOUSE_CLIENT -q "CREATE TABLE q (${cols}) ENGINE = S3Queue('http://localhost:11111/test/', 'user', 'password', CSV) SETTINGS mode = 'ordered', keeper_path = '${keeper_path}', after_processing = 'delete'"

# RESET SETTING must be rejected before Keeper state is changed.
$CLICKHOUSE_CLIENT --max_query_size=1480 -q "ALTER TABLE q RESET SETTING after_processing" 2>&1 | grep -o -F -m1 "QUERY_IS_TOO_LARGE"

# The Keeper metadata must still show after_processing = delete (the rejected reset must not apply).
$CLICKHOUSE_CLIENT -q "SELECT JSONExtractString(value, 'after_processing') FROM system.zookeeper WHERE path = '${keeper_path}' AND name = 'metadata'"

$CLICKHOUSE_CLIENT -q "DROP TABLE q SYNC"

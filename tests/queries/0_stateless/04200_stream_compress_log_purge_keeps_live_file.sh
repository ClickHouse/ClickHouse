#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/130677
# With logger.stream_compress=true and logger.count=1, the purge after a rotation
# must keep both the archive and the live "<path>.lz4" file.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A fresh directory is required, otherwise leftover files from previous runs change the result.
log_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_logs"
rm -rf "$log_dir"

$CLICKHOUSE_LOCAL --query "SELECT 1" -- --logger.log="$log_dir/clickhouse-server.log" --logger.stream_compress=true --logger.rotateOnOpen=true --logger.count=1 >/dev/null 2>&1

ls "$log_dir" | sort

rm -rf "$log_dir"

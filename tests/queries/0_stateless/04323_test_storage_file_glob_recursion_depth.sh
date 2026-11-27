#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 \
    | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

DEEP_DIR_NAME="$CLICKHOUSE_TEST_UNIQUE_NAME-deep"
DEEP_DIR_ABS="$USER_FILES_PATH/$DEEP_DIR_NAME"

mkdir -p "$DEEP_DIR_ABS"
trap 'rm -rf "$DEEP_DIR_ABS"' EXIT

# Build a chain of >1000 nested directories. A trailing `/**` glob walks every
# level, so the listing depth equals the chain length and must trip the guard.
NESTED="$DEEP_DIR_ABS"
for _ in $(seq 1 1100); do
    NESTED="$NESTED/x"
done
mkdir -p "$NESTED"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$DEEP_DIR_NAME/**')" 2>&1 \
    | grep -oF "TOO_DEEP_RECURSION" | head -n 1

$CLICKHOUSE_CLIENT --query "SELECT 'alive'"

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# By default (no --path and no --tmp), clickhouse-local works with a designated data directory
# inside the home directory, so the data survives between runs. Point HOME (and XDG_DATA_HOME)
# at a private directory to observe this without touching the real home directory.
# Note: $CLICKHOUSE_LOCAL is not used here because it passes --tmp.

home_dir="${CLICKHOUSE_TMP}/home_${CLICKHOUSE_DATABASE}"
rm -rf "$home_dir"
mkdir -p "$home_dir"

local_default()
{
    HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} local "$@"
}

# The data survives between separate invocations.
local_default --query "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t VALUES (1)"
local_default --query "SELECT * FROM t"

# The data lives in the designated directory inside the home directory
# (~/.local/share/clickhouse-local on Linux, ~/Library/Application Support/clickhouse-local on macOS).
if [ -d "$home_dir/.local/share/clickhouse-local" ] || [ -d "$home_dir/Library/Application Support/clickhouse-local" ]
then
    echo "data dir created"
fi

# --tmp gives the old behavior: a unique temporary directory, no access to the persistent data.
local_default --tmp --query "EXISTS TABLE t"

# An explicit --path takes precedence over --tmp, so wrappers can pass --tmp as a baseline
# while individual invocations still redirect the data to an explicit location.
path_dir="${CLICKHOUSE_TMP}/path_${CLICKHOUSE_DATABASE}"
rm -rf "$path_dir"
local_default --tmp --path "$path_dir" --query "CREATE TABLE t2 (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t2 VALUES (2)"
local_default --tmp --path "$path_dir" --query "SELECT * FROM t2"

# The table created under --path did not leak into the default directory,
# and the default directory still has its own data.
local_default --query "EXISTS TABLE t2"
local_default --query "SELECT * FROM t"

rm -rf "$home_dir" "$path_dir"

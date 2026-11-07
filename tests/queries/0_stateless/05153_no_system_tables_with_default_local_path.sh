#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `--no-system-tables` option must keep working in the default mode, which uses a designated
# data directory in the home directory. Point HOME (and XDG_DATA_HOME) at a private directory,
# because the default mode locks and loads that directory.
# Note: $CLICKHOUSE_LOCAL is not used here because it passes --tmp.

home_dir="${CLICKHOUSE_TMP}/home_${CLICKHOUSE_DATABASE}"
rm -rf "$home_dir"
mkdir -p "$home_dir"

local_default()
{
    HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} local "$@"
}

# Without the option, the system tables and the information schema are there.
local_default --query "SELECT count() > 0 FROM system.tables WHERE database = 'system'"
local_default --query "SELECT count() > 0 FROM information_schema.tables"

# With the option, neither the system database nor the information schema is attached.
local_default --no-system-tables --query "SELECT count() FROM system.tables" 2>&1 | grep -c -F 'UNKNOWN_DATABASE'
local_default --no-system-tables --query "SELECT count() FROM information_schema.tables" 2>&1 | grep -c -F 'UNKNOWN_DATABASE'

# The user's own tables in the default directory still work with --no-system-tables.
local_default --no-system-tables --query "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t VALUES (1)"
local_default --no-system-tables --query "SELECT * FROM t"

# ... and they are visible in the following invocation, which is the point of the default directory.
local_default --query "SELECT * FROM t"

rm -rf "$home_dir"

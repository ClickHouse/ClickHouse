#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# By default, clickhouse-local keeps its tables in a designated directory inside the home directory,
# so a table that cannot be loaded any more (here: a Set table whose persisted state is corrupted)
# survives between runs. Like a server, clickhouse-local must still run the queries that do not use
# that table, and fail only the ones that do, instead of refusing to run anything.
# Note: $CLICKHOUSE_LOCAL is not used here because it passes --tmp.

home_dir="${CLICKHOUSE_TMP}/home_${CLICKHOUSE_DATABASE}"
rm -rf "$home_dir"
mkdir -p "$home_dir"

local_default()
{
    HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} local "$@"
}

local_default --query "
    CREATE TABLE good (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO good VALUES (1);
    CREATE TABLE broken (x UInt64) ENGINE = Set;
    INSERT INTO broken VALUES (1);
"

# The Set engine reads its persisted state back while the table loads. The data path is reported
# relative to the designated directory, whose location depends on the platform.
broken_data_path=$(local_default --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'broken'")
broken_dir=$(find "$home_dir" -type d -path "*/${broken_data_path%/}")
for f in "$broken_dir"/*.bin; do echo garbage > "$f"; done

# A query that does not use the broken table works, and the load failure is reported once.
stderr_file="${CLICKHOUSE_TMP}/stderr_${CLICKHOUSE_DATABASE}"
local_default --query "SELECT * FROM good" 2> "$stderr_file"
grep -c "^Warning: .*broken" "$stderr_file"
wc -l < "$stderr_file"

# A query that uses the broken table fails with its load error.
local_default --query "SELECT * FROM broken" 2>&1 | grep -v '^Warning: ' | grep -o 'ASYNC_LOAD_WAIT_FAILED'

# The other table is untouched by all of this.
local_default --query "SELECT count() FROM good" 2> /dev/null

rm -rf "$home_dir" "$stderr_file"

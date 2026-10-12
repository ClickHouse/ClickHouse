#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Fast tests don't build external libraries (SQLite)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `sqlite3_open` allocates the connection handle even when it fails to open the database file.
# An empty path resolves to the `user_files` directory, which cannot be opened as a SQLite database,
# so `sqlite3_open` fails after allocating the handle. The handle must be closed to avoid a memory
# leak (detected as a leak under ASan/LSan). See https://www.sqlite.org/c3ref/open.html.
$CLICKHOUSE_CLIENT --query "SELECT * FROM sqlite('', '')" 2>&1 | grep -oF "Cannot access sqlite database" | head -n1

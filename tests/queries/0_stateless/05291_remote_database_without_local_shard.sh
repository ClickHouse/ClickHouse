#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Remote` database whose only shard is remote: counting its rows through `merge` and shutting down
# the process that holds it both make the process list the tables of the database on its own, which is
# a query to the remote server. `clickhouse-local` is that process; this server is the remote one.

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t (x UInt8) ENGINE = Memory; INSERT INTO t VALUES (1), (2)"

${CLICKHOUSE_LOCAL} --query "
    CREATE DATABASE proxy ENGINE = Remote('127.0.0.2:${CLICKHOUSE_PORT_TCP}', '${CLICKHOUSE_DATABASE}');
    SELECT count() FROM merge('proxy', '^t\$') SETTINGS optimize_trivial_count_query = 1;
" 2>&1 | grep -av "ASan doesn't fully support makecontext/swapcontext functions"
echo "exit code: ${PIPESTATUS[0]}"

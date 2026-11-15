#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Concurrent `CREATE OR REPLACE` of the same table in an `Atomic` database must succeed every time.
$CLICKHOUSE_CLIENT --query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_db ENGINE=Atomic"

# The statements are serialized on the table name and can take a second each with the database metadata on a remote disk.
export TIMEOUT=60

function create_or_replace_table_thread
{
    for _ in {1..20}; do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "CREATE OR REPLACE TABLE ${CLICKHOUSE_DATABASE}_db.test_table (x Int) ENGINE=Memory"
        [[ $SECONDS -ge "$TIMEOUT" ]] && break
    done
}
export -f create_or_replace_table_thread;

for _ in {1..20}; do
    bash -c create_or_replace_table_thread &
done

wait

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db SYNC";

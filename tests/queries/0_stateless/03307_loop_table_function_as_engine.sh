#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE test (uid Int16, name String, age Int16) ENGINE=Loop;" 2>&1 | grep -m 1 -cF 'cannot be used to create a table'

# Verify the table was not created
table_exists=$($CLICKHOUSE_CLIENT -q "EXISTS TABLE test" | tr -d '\n')
if [[ $table_exists -eq 1 ]]; then
    echo "ERROR: Table was created but shouldn't exist."
    exit 1
fi

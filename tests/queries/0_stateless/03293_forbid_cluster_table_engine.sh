#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE test AS s3Cluster('test_shard_localhost', 'http://localhost:11111/test/a.tsv', 'TSV', 'a Int32, b Int32, c Int32');" 2>&1 | grep -m 1 -cF 'Cannot create a table with *Cluster engine'

# Verify the table was not created
table_exists=$($CLICKHOUSE_CLIENT -q "EXISTS TABLE test" | tr -d '\n')
if [[ $table_exists -eq 1 ]]; then
    echo "ERROR: Table was created but shouldn't exist."
    exit 1
fi

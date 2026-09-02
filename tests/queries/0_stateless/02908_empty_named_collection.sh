#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

collection_name="$CLICKHOUSE_DATABASE"_foobar03

$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION $collection_name AS a = 1"

$CLICKHOUSE_CLIENT -q "ALTER NAMED COLLECTION $collection_name DELETE b" 2>&1 | grep -q "BAD_ARGUMENTS" || echo "Missing BAD_ARGUMENTS error"

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION $collection_name"

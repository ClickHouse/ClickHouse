#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ALTER NAMED COLLECTION` stores the new value as text. A literal too large for UInt64 resolves to a
# wide integer, and quoting it makes the collection serve the value with the quotes in it until the
# next reload, so the error below named a format that nothing can match.

collection_name="$CLICKHOUSE_DATABASE"_05141

$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION $collection_name AS url = 'http://localhost:1/x', format = 'CSV', structure = 'a UInt8'"
$CLICKHOUSE_CLIENT -q "ALTER NAMED COLLECTION $collection_name SET format = 18446744073709551616"
# The client prints the exception twice, so keep the first line only.
$CLICKHOUSE_CLIENT -q "SELECT * FROM url($collection_name)" 2>&1 | grep -o "Unknown format '\?18446744073709551616'\?" | head -1
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION $collection_name"

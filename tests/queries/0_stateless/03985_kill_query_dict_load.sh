#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan, long
# Test that KILL QUERY works for queries blocked on dictionary loading.
# Ref: https://github.com/ClickHouse/ClickHouse/issues/97559

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="kill_query_dict_load_${CLICKHOUSE_DATABASE}_$RANDOM"

# Create a dictionary with a source query that takes forever to load.
$CLICKHOUSE_CLIENT --query "
    CREATE DICTIONARY IF NOT EXISTS ${CLICKHOUSE_DATABASE}.slow_dict
    (
        id UInt64,
        value String
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(QUERY 'SELECT number AS id, toString(number) AS value FROM system.numbers'))
    LIFETIME(0)
    LAYOUT(HASHED())
"

# This query will block waiting for the dictionary to load (which will never finish).
$CLICKHOUSE_CLIENT --query_id="$query_id" --query "
    SELECT dictGet('${CLICKHOUSE_DATABASE}.slow_dict', 'value', toUInt64(1))
" >/dev/null 2>&1 &

wait_for_query_to_start "$query_id"

# Use async KILL (without SYNC) to avoid blocking if propagation is slow.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

wait

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SYSTEM RELOAD DICTIONARY ${CLICKHOUSE_DATABASE}.slow_dict" >/dev/null 2>&1 || true
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "DROP DICTIONARY IF EXISTS ${CLICKHOUSE_DATABASE}.slow_dict"

echo "OK"

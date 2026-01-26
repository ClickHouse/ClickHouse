#!/usr/bin/env bash
# Tags: race, no-parallel

# Test for race condition when dropping a table while queries are running.
# The issue occurs when a part's destructor calls clearCaches() after the storage
# has been destroyed, causing a segfault when accessing storage.getPrimaryIndexCache().
# Fixed by storing a weak pointer to context in the part.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export TEST_TABLE="${CLICKHOUSE_DATABASE}.test_race_drop_03806"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TEST_TABLE}" 2>/dev/null
}
trap cleanup EXIT

function create_and_fill()
{
    ${CLICKHOUSE_CLIENT} -q "
        CREATE TABLE IF NOT EXISTS ${TEST_TABLE} (
            date Date,
            id UInt64,
            value String
        ) ENGINE = MergeTree()
        PARTITION BY date
        ORDER BY id
        SETTINGS use_primary_key_cache = 1, primary_key_lazy_load = 1
    " 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed" || true

    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO ${TEST_TABLE} SELECT
            today() - (number % 10),
            number,
            'value_' || toString(number)
        FROM numbers(10000)
    " 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed" || true
}

function select_query()
{
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM ${TEST_TABLE} WHERE value LIKE '%5%'
    " 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed" || true
}

function select_distributed()
{
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM remote('127.0.0.1', '${TEST_TABLE}')
    " 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed" || true
}

function drop_table()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TEST_TABLE}" 2>&1 | grep -F "Code: " | grep -Fv "is currently dropped or renamed" || true
}

# Run multiple iterations to increase chance of hitting the race
for _ in {1..20}; do
    create_and_fill

    # Run queries and drop concurrently
    for _ in {1..5}; do
        select_query &
        select_distributed &
    done
    drop_table &

    wait
done

echo "OK"

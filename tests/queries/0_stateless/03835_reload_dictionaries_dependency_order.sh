#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

# Verify that SYSTEM RELOAD DICTIONARIES reloads dictionaries in topological order,
# so that dictionaries sourcing from other dictionaries see fresh data.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS test_dict_deps"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE test_dict_deps"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_dict_deps.source (id UInt64, value String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO test_dict_deps.source VALUES (1, 'a'), (2, 'b'), (3, 'c')"

# Chain: source -> d1 -> d2 -> d3
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY test_dict_deps.d1 (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'source' DB 'test_dict_deps'))
    LIFETIME(0)
    LAYOUT(FLAT())
"

$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY test_dict_deps.d2 (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'd1' DB 'test_dict_deps'))
    LIFETIME(0)
    LAYOUT(FLAT())
"

$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY test_dict_deps.d3 (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'd2' DB 'test_dict_deps'))
    LIFETIME(0)
    LAYOUT(FLAT())
"

# Trigger initial load of the entire chain
$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_dict_deps.d3"

# Insert more data into the source table
$CLICKHOUSE_CLIENT -q "INSERT INTO test_dict_deps.source VALUES (4, 'd'), (5, 'e')"

# Reload all dictionaries; with topological ordering d1 reloads before d2 before d3
$CLICKHOUSE_CLIENT -q "SYSTEM RELOAD DICTIONARIES"

# All dictionaries should see the updated 5 rows
$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_dict_deps.d1"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_dict_deps.d2"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_dict_deps.d3"

$CLICKHOUSE_CLIENT -q "DROP DATABASE test_dict_deps"

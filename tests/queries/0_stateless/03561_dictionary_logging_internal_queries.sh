#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that internal queries from dictionaries are logged correctly
# We test multiple dictionary layouts that use the ClickHouse source.

# Generate a unique run ID for this test execution
RUN_ID="run_${RANDOM}_${RANDOM}_$$"

SIMPLE_KEY_LAYOUTS=("FLAT()" "HASHED()" "HASHED_ARRAY()" "SPARSE_HASHED()")

for layout in "${SIMPLE_KEY_LAYOUTS[@]}"; do

$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict
(
    name String,
    value Float64
)
PRIMARY KEY name
SOURCE(
    CLICKHOUSE(
        QUERY 'SELECT toString(number) AS name, toFloat64(number) AS value FROM numbers(10) WHERE ''$layout-$RUN_ID'' != '''''
    )
)
LIFETIME(30)
LAYOUT($layout)"

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"

$CLICKHOUSE_CLIENT --query "
SELECT countIf(type = 'QueryStart'), countIf(type = 'QueryFinish')
FROM system.query_log
WHERE 1
    AND is_internal = 1
    AND query = 'SELECT toString(number) AS name, toFloat64(number) AS value FROM numbers(10) WHERE ''$layout-$RUN_ID'' != '''''
    AND current_database IN ['default', currentDatabase()]"

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict"
done


# COMPLEX_KEY_HASHED
$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict
(
    key1 String,
    key2 UInt64,
    value Float64
)
PRIMARY KEY key1, key2
SOURCE(
    CLICKHOUSE(
        QUERY 'SELECT toString(number) AS key1, number AS key2, toFloat64(number) AS value FROM numbers(10) WHERE ''COMPLEX_KEY_HASHED()-$RUN_ID'' != '''''
    )
)
LIFETIME(30)
LAYOUT(COMPLEX_KEY_HASHED())"

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"

$CLICKHOUSE_CLIENT --query "
SELECT countIf(type = 'QueryStart'), countIf(type = 'QueryFinish')
FROM system.query_log
WHERE 1
    AND is_internal = 1
    AND query = 'SELECT toString(number) AS key1, number AS key2, toFloat64(number) AS value FROM numbers(10) WHERE ''COMPLEX_KEY_HASHED()-$RUN_ID'' != '''''
    AND current_database IN ['default', currentDatabase()]"

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict"


# DIRECT
$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict
(
    name String,
    value Float64
)
PRIMARY KEY name
SOURCE(
    CLICKHOUSE(
        QUERY 'SELECT toString(number) AS name, toFloat64(number) AS value FROM numbers(10) WHERE ''DIRECT()-$RUN_ID'' != '''''
    )
)
LAYOUT(DIRECT())"

$CLICKHOUSE_CLIENT --query "SELECT * FROM ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"

$CLICKHOUSE_CLIENT --query "
SELECT countIf(type = 'QueryStart'), countIf(type = 'QueryFinish')
FROM system.query_log
WHERE 1
    AND is_internal = 1
    AND query = 'SELECT toString(number) AS name, toFloat64(number) AS value FROM numbers(10) WHERE ''DIRECT()-$RUN_ID'' != '''''
    AND current_database IN ['default', currentDatabase()]"

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict"


# DIRECT BROKEN
$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict
(
    name Int8
)
PRIMARY KEY name
SOURCE(
    CLICKHOUSE(
        QUERY 'SELECT number FROM numbers(1) WHERE throwIf(number = 0, ''ERROR'') AND ''DIRECT()-$RUN_ID'' != '''''
    )
)
LAYOUT(DIRECT())"

$CLICKHOUSE_CLIENT --query "SELECT * FROM ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict FORMAT Null" 2>/dev/null

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"

$CLICKHOUSE_CLIENT --query "
SELECT countIf(type = 'ExceptionWhileProcessing')
FROM system.query_log
WHERE 1
    AND is_internal = 1
    AND query = 'SELECT number FROM numbers(1) WHERE throwIf(number = 0, ''ERROR'') AND ''DIRECT()-$RUN_ID'' != '''''
    AND current_database IN ['default', currentDatabase()]"

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict"

#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest, no-llvm-coverage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that internal queries from dictionaries are logged correctly
# We test multiple dictionary layouts that use the ClickHouse source.

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
    LIFETIME(MIN 100500 MAX 100500)
    LAYOUT($layout)"

    $CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict"
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
LIFETIME(MIN 100500 MAX 100500)
LAYOUT(COMPLEX_KEY_HASHED())"

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict"
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
$CLICKHOUSE_CLIENT --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict"


$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
SELECT
    countIf(query LIKE '%FLAT()-%' AND type = 'QueryStart'),
    countIf(query LIKE '%FLAT()-%' AND type = 'QueryFinish'),
    countIf(query LIKE '%''HASHED()-%' AND type = 'QueryStart'),
    countIf(query LIKE '%''HASHED()-%' AND type = 'QueryFinish'),
    countIf(query LIKE '%HASHED_ARRAY()-%' AND type = 'QueryStart'),
    countIf(query LIKE '%HASHED_ARRAY()-%' AND type = 'QueryFinish'),
    countIf(query LIKE '%SPARSE_HASHED()-%' AND type = 'QueryStart'),
    countIf(query LIKE '%SPARSE_HASHED()-%' AND type = 'QueryFinish'),
    countIf(query LIKE '%COMPLEX_KEY_HASHED()-%' AND type = 'QueryStart'),
    countIf(query LIKE '%COMPLEX_KEY_HASHED()-%' AND type = 'QueryFinish'),
    countIf(query LIKE '%DIRECT()-%' AND type = 'QueryStart'),
    countIf(query LIKE '%DIRECT()-%' AND type = 'QueryFinish'),
    countIf(query LIKE '%DIRECT()-%' AND type = 'ExceptionWhileProcessing')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND is_internal = 1
    AND query LIKE '%$RUN_ID%'
    AND current_database IN ['default', currentDatabase()]"


# Test that trace_log entries from dictionary auto-reload have non-empty query_id
# matching internal queries in query_log.

$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict
(
    n UInt64
)
PRIMARY KEY n
SOURCE(CLICKHOUSE(
    QUERY 'SELECT 0 AS n WHERE sleep(3) = 0 AND ''PROFILER_TEST-$RUN_ID'' != '''''
))
LIFETIME(MIN 1 MAX 1)
LAYOUT(FLAT())"

# Force initial load, then wait for at least one auto-reload by polling system.dictionaries.
$CLICKHOUSE_CLIENT --query "SELECT dictHas('${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict', toUInt64(0)) FORMAT Null"

INITIAL_LOAD_TIME=$($CLICKHOUSE_CLIENT --query "SELECT last_successful_update_time FROM system.dictionaries WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'test_logging_internal_queries_dict'")
for _ in $(seq 1 60); do
    CURRENT_LOAD_TIME=$($CLICKHOUSE_CLIENT --query "SELECT last_successful_update_time FROM system.dictionaries WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'test_logging_internal_queries_dict'")
    if [ "$CURRENT_LOAD_TIME" != "$INITIAL_LOAD_TIME" ]; then
        break
    fi
    sleep 1
done

if [ "$CURRENT_LOAD_TIME" = "$INITIAL_LOAD_TIME" ]; then
    echo "Dictionary did not reload within 60 seconds"
fi

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY ${CLICKHOUSE_DATABASE}.test_logging_internal_queries_dict"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, trace_log"

$CLICKHOUSE_CLIENT --query "
SELECT count() > 0
FROM system.trace_log
WHERE trace_type = 'Real'
    AND query_id IN (
        SELECT query_id
        FROM system.query_log
        WHERE is_internal = 1
            AND query LIKE '%PROFILER_TEST-$RUN_ID%'
            AND type = 'QueryFinish'
            AND current_database IN ['default', currentDatabase()]
    )"

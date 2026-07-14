#!/usr/bin/env bash
# Tags: no-fasttest
# Test http_column_* URL params: map HTTP request headers to INSERT columns.
# Works for both sync and async inserts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS test_http_columns;
    CREATE TABLE test_http_columns (
        event_type LowCardinality(String),
        signature String,
        payload String
    ) ENGINE = MergeTree ORDER BY tuple();
"

echo "--- sync: no explicit column list (body provides remaining columns)"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: push' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
    -d '{"payload":"no-list","signature":"s"}'

${CLICKHOUSE_CLIENT} -q "SELECT event_type, signature, payload FROM test_http_columns"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns"

echo "--- sync: basic header-to-column mapping"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: push' \
    -H 'X-Signature: sha256=abc123' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&http_column_X-Signature=signature" \
    -d '{"payload":"hello"}'

${CLICKHOUSE_CLIENT} -q "SELECT event_type, signature, payload FROM test_http_columns ORDER BY payload"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns"

echo "--- sync: multiple rows"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: release' \
    -H 'X-Signature: sha256=def456' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&http_column_X-Signature=signature" \
    -d '{"payload":"row1"}
{"payload":"row2"}
{"payload":"row3"}'

${CLICKHOUSE_CLIENT} -q "SELECT event_type, signature, payload FROM test_http_columns ORDER BY payload"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns"

echo "--- sync: case-insensitive header name"
${CLICKHOUSE_CURL} -sS \
    -H 'x-event-type: issues' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
    -d '{"payload":"case-test"}'

${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM test_http_columns"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns"

echo "--- sync: missing header produces empty string"
${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
    -d '{"payload":"no-header"}'

${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM test_http_columns"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns"

echo "--- async: no explicit column list (body provides remaining columns)"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: push' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+test_http_columns+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
    -d '{"payload":"no-list-async","signature":"s"}'

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
${CLICKHOUSE_CLIENT} -q "SELECT event_type, signature, payload FROM test_http_columns"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns"

echo "--- async: different header values per request coalesce into one batch"
# Two fire-and-forget requests, then explicit flush. Each row must carry its own
# request's header values to verify per-entry injection works correctly.
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: push' \
    -H 'X-Signature: sig1' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+test_http_columns+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&http_column_X-Signature=signature" \
    -d '{"payload":"async1"}'

${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: release' \
    -H 'X-Signature: sig2' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+test_http_columns+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&http_column_X-Signature=signature" \
    -d '{"payload":"async2"}'

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
${CLICKHOUSE_CLIENT} -q "SELECT event_type, signature, payload FROM test_http_columns ORDER BY payload"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns"

echo "--- async: multiple rows per entry"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: star' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+test_http_columns+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
    -d '{"payload":"multi1"}
{"payload":"multi2"}'

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM test_http_columns ORDER BY payload"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns"

echo "--- error: column listed in both INSERT list and http_column_*"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: push' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns+(event_type,payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
    -d '{"payload":"conflict"}' 2>&1 | grep -o 'DUPLICATE_COLUMN'

echo "--- error: non-existent column"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: push' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=no_such_column" \
    -d '{"payload":"err"}' 2>&1 | grep -o 'NO_SUCH_COLUMN_IN_TABLE'

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_http_columns"

# MATERIALIZED and ALIAS columns are not insertable and must be rejected.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS test_http_columns_non_insertable;
    CREATE TABLE test_http_columns_non_insertable (
        payload   String,
        mat_col   UInt64 MATERIALIZED length(payload),
        alias_col String ALIAS payload
    ) ENGINE = MergeTree ORDER BY tuple();
"

echo "--- error: MATERIALIZED column (sync)"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Val: 42' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns_non_insertable+(payload)+FORMAT+JSONEachRow&http_column_X-Val=mat_col" \
    -d '{"payload":"x"}' 2>&1 | grep -o 'NO_SUCH_COLUMN_IN_TABLE'

echo "--- error: ALIAS column (sync)"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Val: hello' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns_non_insertable+(payload)+FORMAT+JSONEachRow&http_column_X-Val=alias_col" \
    -d '{"payload":"x"}' 2>&1 | grep -o 'NO_SUCH_COLUMN_IN_TABLE'

echo "--- error: MATERIALIZED column (async)"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Val: 42' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+test_http_columns_non_insertable+(payload)+FORMAT+JSONEachRow&http_column_X-Val=mat_col" \
    -d '{"payload":"x"}' 2>&1 | grep -o 'NO_SUCH_COLUMN_IN_TABLE'

echo "--- error: ALIAS column (async)"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Val: hello' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+test_http_columns_non_insertable+(payload)+FORMAT+JSONEachRow&http_column_X-Val=alias_col" \
    -d '{"payload":"x"}' 2>&1 | grep -o 'NO_SUCH_COLUMN_IN_TABLE'

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_http_columns_non_insertable"

# Test non-String column types: values are deserialized through the column type's
# text serialization, so UInt64, Array, Date, etc. should parse correctly.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS test_http_columns_typed;
    CREATE TABLE test_http_columns_typed (
        count UInt64,
        tags Array(String),
        rate Float64,
        payload String
    ) ENGINE = MergeTree ORDER BY tuple();
"

echo "--- sync: non-String column types"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Count: 42' \
    -H "X-Tags: ['important','urgent']" \
    -H 'X-Rate: 3.14' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns_typed+(payload)+FORMAT+JSONEachRow&http_column_X-Count=count&http_column_X-Tags=tags&http_column_X-Rate=rate" \
    -d '{"payload":"typed-test"}'

${CLICKHOUSE_CLIENT} -q "SELECT count, tags, rate, payload FROM test_http_columns_typed"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns_typed"

echo "--- async: non-String column types"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Count: 100' \
    -H "X-Tags: ['async']" \
    -H 'X-Rate: 2.72' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+test_http_columns_typed+(payload)+FORMAT+JSONEachRow&http_column_X-Count=count&http_column_X-Tags=tags&http_column_X-Rate=rate" \
    -d '{"payload":"async-typed"}'

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
${CLICKHOUSE_CLIENT} -q "SELECT count, tags, rate, payload FROM test_http_columns_typed"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_http_columns_typed"

# Test INSERT INTO FUNCTION remote() -- verifies that table functions are supported
# and resolved correctly (not broken by direct DatabaseCatalog lookup).
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS test_http_columns_remote;
    CREATE TABLE test_http_columns_remote (
        event_type String,
        payload    String
    ) ENGINE = MergeTree ORDER BY tuple();
"

echo "--- sync: INSERT INTO FUNCTION remote()"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: push' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+FUNCTION+remote('127.0.0.1',currentDatabase(),test_http_columns_remote)+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
    -d '{"payload":"remote-sync"}'

${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM test_http_columns_remote"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns_remote"

echo "--- async: INSERT INTO FUNCTION remote()"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: release' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+FUNCTION+remote('127.0.0.1',currentDatabase(),test_http_columns_remote)+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
    -d '{"payload":"remote-async"}'

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM test_http_columns_remote"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_http_columns_remote"

# Test DEFAULT expressions that reference an http_column_* injected column.
# With input_format_defaults_for_omitted_fields=1, the DEFAULT expression
# `b DEFAULT a + 1` must use the injected value of `a`, not zero.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS test_http_columns_defaults;
    CREATE TABLE test_http_columns_defaults (
        a UInt64,
        b UInt64 DEFAULT a + 1,
        payload String
    ) ENGINE = MergeTree ORDER BY tuple();
"

echo "--- sync: DEFAULT expression referencing injected column"
${CLICKHOUSE_CURL} -sS \
    -H 'X-A: 5' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+test_http_columns_defaults+(payload)+FORMAT+JSONEachRow&http_column_X-A=a&input_format_defaults_for_omitted_fields=1" \
    -d '{"payload":"sync-default"}'

${CLICKHOUSE_CLIENT} -q "SELECT a, b, payload FROM test_http_columns_defaults"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE test_http_columns_defaults"

echo "--- async: DEFAULT expression referencing injected column"
${CLICKHOUSE_CURL} -sS \
    -H 'X-A: 10' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+test_http_columns_defaults+(payload)+FORMAT+JSONEachRow&http_column_X-A=a&input_format_defaults_for_omitted_fields=1" \
    -d '{"payload":"async-default"}'

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
${CLICKHOUSE_CLIENT} -q "SELECT a, b, payload FROM test_http_columns_defaults"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_http_columns_defaults"

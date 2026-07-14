#!/usr/bin/env bash
# Tags: no-fasttest
# Test http_column_* URL params: map HTTP request headers to INSERT columns.
# Works for both sync and async inserts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Helper: build mode-specific URL params and optional flush call.
# Usage: insert_url <extra_params>
# Side effect: sets $INSERT_EXTRA (URL params string) and defines flush()
run_modes() {
    for mode in sync async; do
        if [ "$mode" = "async" ]; then
            INSERT_EXTRA="&async_insert=1&wait_for_async_insert=0"
            flush() { ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"; }
        else
            INSERT_EXTRA=""
            flush() { :; }
        fi
        "$@"
    done
}

# ── Main table ─────────────────────────────────────────────────────────────────
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (
        event_type LowCardinality(String),
        signature  String,
        payload    String
    ) ENGINE = MergeTree ORDER BY tuple();
"

do_basic_tests() {
    echo "--- ${mode}: no explicit column list (body provides remaining columns)"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: push' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
        -d '{"payload":"no-list","signature":"s"}'
    # Note: both sync and async use same payload value for consistent reference output
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, signature, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: no explicit column list, body also contains a header-mapped field (header wins)"
    # Body provides event_type='from-body', but http_column also maps event_type from header.
    # Since event_type is excluded from format_header, the body value is silently discarded
    # and the header value wins. input_format_skip_unknown_fields=1 prevents a parse error.
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: from-header' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&input_format_skip_unknown_fields=1" \
        -d '{"event_type":"from-body","payload":"conflict-test","signature":"s"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: basic header-to-column mapping"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: push' \
        -H 'X-Signature: sha256=abc123' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&http_column_X-Signature=signature" \
        -d '{"payload":"hello"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, signature, payload FROM t ORDER BY payload"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: multiple rows"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: release' \
        -H 'X-Signature: sha256=def456' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&http_column_X-Signature=signature" \
        -d '{"payload":"row1"}
{"payload":"row2"}
{"payload":"row3"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, signature, payload FROM t ORDER BY payload"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: case-insensitive header name"
    ${CLICKHOUSE_CURL} -sS \
        -H 'x-event-type: issues' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
        -d '{"payload":"case-test"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: missing header produces empty string"
    ${CLICKHOUSE_CURL} -sS \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
        -d '{"payload":"no-header"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: positional format (TSV) - body-only columns, header injected separately"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: tsv-push' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+TSV&http_column_X-Event-Type=event_type" \
        -d 'hello-tsv'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: filtered header (Authorization) produces empty string"
    ${CLICKHOUSE_CURL} -sS \
        -H 'Authorization: Bearer secret-token' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_Authorization=event_type" \
        -d '{"payload":"filtered"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: empty header name is ignored (no mapping applied)"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: ignored' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(event_type,payload)+FORMAT+JSONEachRow&http_column_=event_type" \
        -d '{"event_type":"body-value","payload":"empty-hdr"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: empty column name is ignored (no mapping applied)"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: ignored' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(event_type,payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=" \
        -d '{"event_type":"body-value2","payload":"empty-col"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: duplicate http_column_* to same column, first wins"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-A: first' \
        -H 'X-B: second' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-A=event_type&http_column_X-B=event_type" \
        -d '{"payload":"dup"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: type parse error surfaces immediately"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Count: not-a-number' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+typed+(payload)+FORMAT+JSONEachRow&http_column_X-Count=count" \
        -d '{"payload":"type-err"}' 2>&1 | grep -o 'CANNOT_PARSE_TEXT\|Cannot parse'

    echo "--- ${mode}: INSERT ... SELECT rejected"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: push' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+SELECT+%27x%27&http_column_X-Event-Type=event_type" \
        2>&1 | grep -o 'NOT_IMPLEMENTED'
}

# ── Typed table (for type-error test) ─────────────────────────────────────────
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS typed;
    CREATE TABLE typed (count UInt64, payload String) ENGINE = MergeTree ORDER BY tuple();
"

run_modes do_basic_tests

${CLICKHOUSE_CLIENT} -q "DROP TABLE t"
${CLICKHOUSE_CLIENT} -q "DROP TABLE typed"

# ── Async-only: batching with different headers per entry ──────────────────────
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (event_type String, signature String, payload String)
    ENGINE = MergeTree ORDER BY tuple();
"

echo "--- async: different header values per request coalesce into one batch"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: push' -H 'X-Signature: sig1' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&http_column_X-Signature=signature" \
    -d '{"payload":"batch1"}'

${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: release' -H 'X-Signature: sig2' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&http_column_X-Signature=signature" \
    -d '{"payload":"batch2"}'

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
${CLICKHOUSE_CLIENT} -q "SELECT event_type, signature, payload FROM t ORDER BY payload"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

# ── Error cases (format-independent, run once) ─────────────────────────────────
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (
        payload   String,
        mat_col   UInt64 MATERIALIZED length(payload),
        alias_col String ALIAS payload
    ) ENGINE = MergeTree ORDER BY tuple();
"

echo "--- error: column listed in both INSERT list and http_column_*"
${CLICKHOUSE_CURL} -sS \
    -H 'X-E: push' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-E=payload" \
    -d '{"payload":"conflict"}' 2>&1 | grep -o 'DUPLICATE_COLUMN'

echo "--- error: non-existent column"
${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-E=no_col" \
    -d '{"payload":"x"}' 2>&1 | grep -o 'NO_SUCH_COLUMN_IN_TABLE'

echo "--- error: MATERIALIZED column without insert_allow_materialized_columns"
${CLICKHOUSE_CURL} -sS -H 'X-V: 1' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-V=mat_col" \
    -d '{"payload":"x"}' 2>&1 | grep -o 'ILLEGAL_COLUMN'

echo "--- error: ALIAS column is never insertable"
${CLICKHOUSE_CURL} -sS -H 'X-V: hi' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-V=alias_col" \
    -d '{"payload":"x"}' 2>&1 | grep -o 'ILLEGAL_COLUMN'

do_materialized_tests() {
    echo "--- ${mode}: MATERIALIZED column allowed with insert_allow_materialized_columns=1"
    ${CLICKHOUSE_CURL} -sS -H 'X-Mat: 42' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&insert_allow_materialized_columns=1&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Mat=mat_col" \
        -d '{"payload":"mat-test"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT payload, mat_col FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"
}

run_modes do_materialized_tests

${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

# ── Non-String types ───────────────────────────────────────────────────────────
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (count UInt64, tags Array(String), rate Float64, payload String)
    ENGINE = MergeTree ORDER BY tuple();
"

do_typed_tests() {
    echo "--- ${mode}: non-String column types"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Count: 42' \
        -H "X-Tags: ['a','b']" \
        -H 'X-Rate: 3.14' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Count=count&http_column_X-Tags=tags&http_column_X-Rate=rate" \
        -d '{"payload":"typed"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT count, tags, rate, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"
}

run_modes do_typed_tests
${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

# ── INSERT INTO FUNCTION remote() ─────────────────────────────────────────────
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (event_type String, payload String) ENGINE = MergeTree ORDER BY tuple();
"

do_remote_tests() {
    echo "--- ${mode}: INSERT INTO FUNCTION remote()"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: remote-event' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+FUNCTION+remote('127.0.0.1',currentDatabase(),t)+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
        -d '{"payload":"remote"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"
}

run_modes do_remote_tests
${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

# ── DEFAULT expression referencing injected column ────────────────────────────
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt64, b UInt64 DEFAULT a + 1, payload String)
    ENGINE = MergeTree ORDER BY tuple();
"

do_default_tests() {
    echo "--- ${mode}: DEFAULT expression referencing injected column (explicit column list)"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-A: 5' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-A=a&input_format_defaults_for_omitted_fields=1" \
        -d '{"payload":"default-test"}'
    flush

    echo "--- ${mode}: DEFAULT expression referencing injected column (no explicit column list)"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-A: 7' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+FORMAT+JSONEachRow&http_column_X-A=a&input_format_defaults_for_omitted_fields=1" \
        -d '{"payload":"default-no-list"}'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT a, b, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"
}

run_modes do_default_tests
${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

#!/usr/bin/env bash
# Tags: no-fasttest
# Test http_column_* URL params: map HTTP request headers to INSERT columns.
# Works for both sync and async inserts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Assert that stdin contains an expected token and print it. If nothing matches
# (the insert unexpectedly succeeded, or failed with a different message), print
# a visible NO_MATCH marker so the reference diff fails instead of silently
# passing on empty output (a plain `grep | head -1` would hide it).
expect_match() {
    local pattern="$1"
    local input
    input=$(cat)
    local m
    m=$(printf '%s' "$input" | grep -oE "$pattern" | head -1)
    if [ -n "$m" ]; then
        printf '%s\n' "$m"
    else
        printf 'NO_MATCH\n'
    fi
}

# Helper: build mode-specific URL params and optional flush call.
# Usage: insert_url <extra_params>
# Side effect: sets $INSERT_EXTRA (URL params string) and defines flush()
run_modes() {
    for mode in sync async; do
        if [ "$mode" = "async" ]; then
            INSERT_EXTRA="&async_insert=1&wait_for_async_insert=0"
            flush() { ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${CLICKHOUSE_DATABASE}.t"; }
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

    echo "--- ${mode}: no explicit column list, body also contains a header-mapped field is an error"
    # Body provides event_type='from-body' but http_column also maps event_type from header.
    # Even with input_format_skip_unknown_fields=1, the body field is never silently dropped:
    # http_column_* mapped columns are forbidden unknown fields and always raise an error.
    # Use wait_for_async_insert=1 in async mode so the flush-time rejection reaches the client.
    CONFLICT_EXTRA=""
    [ "$mode" = "async" ] && CONFLICT_EXTRA="&async_insert=1&wait_for_async_insert=1"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: from-header' \
        "${CLICKHOUSE_URL}${CONFLICT_EXTRA}&query=INSERT+INTO+t+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&input_format_skip_unknown_fields=1" \
        -d '{"event_type":"from-body","payload":"conflict-test","signature":"s"}' \
        | expect_match 'INCORRECT_DATA'

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

    echo "--- ${mode}: missing header is rejected"
    ${CLICKHOUSE_CURL} -sS \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
        -d '{"payload":"no-header"}' 2>&1 | expect_match 'BAD_QUERY_PARAMETER'

    echo "--- ${mode}: positional format (TSV) - body-only columns, header injected separately"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: tsv-push' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+TSV&http_column_X-Event-Type=event_type" \
        -d 'hello-tsv'
    flush
    ${CLICKHOUSE_CLIENT} -q "SELECT event_type, payload FROM t"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"

    echo "--- ${mode}: filtered header (Authorization) is rejected"
    ${CLICKHOUSE_CURL} -sS \
        -H 'Authorization: Basic ZGVmYXVsdDo=' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_Authorization=event_type" \
        -d '{"payload":"filtered"}' 2>&1 | expect_match 'BAD_QUERY_PARAMETER'

    echo "--- ${mode}: empty header name is rejected"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: ignored' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(event_type,payload)+FORMAT+JSONEachRow&http_column_=event_type" \
        -d '{"event_type":"body-value","payload":"empty-hdr"}' 2>&1 | expect_match 'BAD_QUERY_PARAMETER'

    echo "--- ${mode}: empty column name is rejected"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: ignored' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(event_type,payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=" \
        -d '{"event_type":"body-value2","payload":"empty-col"}' 2>&1 | expect_match 'BAD_QUERY_PARAMETER'

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
        -d '{"payload":"type-err"}' 2>&1 | expect_match 'BAD_QUERY_PARAMETER'

    echo "--- ${mode}: INSERT ... SELECT rejected"
    ${CLICKHOUSE_CURL} -sS \
        -H 'X-Event-Type: push' \
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+t+(payload)+SELECT+%27x%27&http_column_X-Event-Type=event_type" \
        2>&1 | expect_match 'NOT_IMPLEMENTED'
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

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${CLICKHOUSE_DATABASE}.t"
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

# A dedicated table that actually has the mapped column, so these cases reach the
# format's unknown-field guard instead of failing earlier with NO_SUCH_COLUMN.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_conflict;
    CREATE TABLE t_conflict (event_type String, payload String) ENGINE = MergeTree ORDER BY tuple();
"

echo "--- error: CSVWithNames body header contains http_column_* target (skip_unknown_fields=1 must not bypass)"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: push' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t_conflict+(payload)+FORMAT+CSVWithNames&http_column_X-Event-Type=event_type&input_format_skip_unknown_fields=1" \
    --data-binary $'event_type,payload\nfrom-body,conflict' \
    | expect_match 'INCORRECT_DATA'

echo "--- error: case-insensitive body field matches http_column_* target"
${CLICKHOUSE_CURL} -sS \
    -H 'X-Event-Type: push' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t_conflict+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type&input_format_skip_unknown_fields=1&input_format_column_name_matching_mode=ignore_case" \
    -d '{"EVENT_TYPE":"from-body","payload":"conflict"}' \
    | expect_match 'INCORRECT_DATA'
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_conflict"

echo "--- error: column listed in both INSERT list and http_column_*"
${CLICKHOUSE_CURL} -sS \
    -H 'X-E: push' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-E=payload" \
    -d '{"payload":"conflict"}' 2>&1 | expect_match 'DUPLICATE_COLUMN'

echo "--- error: non-existent column"
${CLICKHOUSE_CURL} -sS -H 'X-E: val' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-E=no_col" \
    -d '{"payload":"x"}' 2>&1 | expect_match 'NO_SUCH_COLUMN_IN_TABLE'

echo "--- error: MATERIALIZED column without insert_allow_materialized_columns"
${CLICKHOUSE_CURL} -sS -H 'X-V: 1' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-V=mat_col" \
    -d '{"payload":"x"}' 2>&1 | expect_match 'ILLEGAL_COLUMN'

echo "--- error: ALIAS column is never insertable"
${CLICKHOUSE_CURL} -sS -H 'X-V: hi' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-V=alias_col" \
    -d '{"payload":"x"}' 2>&1 | expect_match 'ILLEGAL_COLUMN'

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
        "${CLICKHOUSE_URL}${INSERT_EXTRA}&query=INSERT+INTO+FUNCTION+remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}',currentDatabase(),t)+(payload)+FORMAT+JSONEachRow&http_column_X-Event-Type=event_type" \
        -d '{"payload":"remote"}'
    flush
    # The remote() call itself may be queued asynchronously on the receiving side,
    # so flush a second time to drain that entry too.
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
    ${CLICKHOUSE_CLIENT} -q "SELECT a, b, payload FROM t ORDER BY payload"
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t"
}

run_modes do_default_tests

# Sync-only: a bad header value with input_format_defaults_for_omitted_fields=1
# must still produce BAD_QUERY_PARAMETER. The table has b DEFAULT a+1, so
# columns->hasDefaults() is true and getSourceFromASTInsertQuery also parses the
# header value. HTTPHeaderColumnsTransform is constructed first, so its
# BAD_QUERY_PARAMETER wrap fires before the raw parse in getSourceFromInputFormat.
echo "--- sync: bad header value with input_format_defaults_for_omitted_fields=1"
${CLICKHOUSE_CURL} -sS \
    -H 'X-A: not-a-number' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-A=a&input_format_defaults_for_omitted_fields=1" \
    -d '{"payload":"x"}' \
    | expect_match 'BAD_QUERY_PARAMETER'

${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

# ── Async schema drift and per-entry failure isolation ────────────────────────
# Entries are buffered until we ALTER the column type and call
# SYSTEM FLUSH ASYNC INSERT QUEUE manually. A large fixed busy timeout with the
# adaptive timeout disabled keeps entries in the queue even when the flaky check
# randomizes async insert settings; otherwise an early flush could materialize
# 'abcd' before the ALTER and make the ALTER itself fail with TOO_LARGE_STRING_SIZE.
ASYNC_BUF="async_insert=1&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_min_ms=300000&async_insert_busy_timeout_max_ms=300000"
ASYNC_BUF_WAIT="async_insert=1&wait_for_async_insert=1&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_min_ms=300000&async_insert_busy_timeout_max_ms=300000"

# Test 0: multi-header, middle-entry failure isolation.
# Three entries, two mapped headers (code String + tag String).
# After ALTER to FixedString(2), the middle entry's code value ('abcd', 4 chars)
# no longer fits. Entries 1 and 3 must survive with their own header values.
# String (not FixedString(4)) is used as the initial type because ALTER to a
# smaller FixedString checks the metadata sample block (which is 4 null bytes
# for FixedString(4) and can't fit in FixedString(2)), making the ALTER itself
# fail even on an empty table. String's default '' fits any FixedString.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (code String, tag String, payload String)
    ENGINE = MergeTree ORDER BY tuple();
"

${CLICKHOUSE_CURL} -sS \
    -H 'X-Code: aa' -H 'X-Tag: first' \
    "${CLICKHOUSE_URL}&${ASYNC_BUF}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Code=code&http_column_X-Tag=tag" \
    -d '{"payload":"entry1"}'

${CLICKHOUSE_CURL} -sS \
    -H 'X-Code: abcd' -H 'X-Tag: second' \
    "${CLICKHOUSE_URL}&${ASYNC_BUF}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Code=code&http_column_X-Tag=tag" \
    -d '{"payload":"entry2"}'

${CLICKHOUSE_CURL} -sS \
    -H 'X-Code: bb' -H 'X-Tag: third' \
    "${CLICKHOUSE_URL}&${ASYNC_BUF}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Code=code&http_column_X-Tag=tag" \
    -d '{"payload":"entry3"}'

echo "--- async: multi-header middle-entry failure isolation (alter)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t MODIFY COLUMN code FixedString(2)"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${CLICKHOUSE_DATABASE}.t"

echo "--- async: multi-header middle-entry failure isolation (results)"
${CLICKHOUSE_CLIENT} -q "SELECT code, tag, payload FROM t ORDER BY payload"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

# Test 1: String -> FixedString(2) drift.
# Both entries are enqueued before the ALTER; the one with the 4-char value must
# fail in isolation while the 2-char entry survives.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (code String, payload String) ENGINE = MergeTree ORDER BY tuple();
"

${CLICKHOUSE_CURL} -sS \
    -H 'X-Code: ab' \
    "${CLICKHOUSE_URL}&${ASYNC_BUF}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Code=code" \
    -d '{"payload":"short-valid"}'

${CLICKHOUSE_CURL} -sS \
    -H 'X-Code: abcd' \
    "${CLICKHOUSE_URL}&${ASYNC_BUF}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Code=code" \
    -d '{"payload":"exact-valid"}'

echo "--- async: schema drift same TypeIndex (FixedString shrink) (alter)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t MODIFY COLUMN code FixedString(2)"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${CLICKHOUSE_DATABASE}.t"

echo "--- async: schema drift same TypeIndex (FixedString shrink) - valid entry survives"
${CLICKHOUSE_CLIENT} -q "SELECT code, payload FROM t ORDER BY payload"

# Test 2: Dedup — a failed entry's token must remain retryable.
# Both entries are enqueued BEFORE the ALTER that invalidates 'abcd'.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (code String, payload String) ENGINE = MergeTree ORDER BY tuple();
"

${CLICKHOUSE_CURL} -sS \
    -H 'X-Code: ab' \
    "${CLICKHOUSE_URL}&${ASYNC_BUF}&insert_deduplication_token=token-valid&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Code=code" \
    -d '{"payload":"dedup-valid"}'

${CLICKHOUSE_CURL} -sS \
    -H 'X-Code: abcd' \
    "${CLICKHOUSE_URL}&${ASYNC_BUF}&insert_deduplication_token=token-invalid&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Code=code" \
    -d '{"payload":"dedup-invalid"}'

echo "--- async: failed dedup token remains retryable after schema drift (alter)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t MODIFY COLUMN code FixedString(2)"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${CLICKHOUSE_DATABASE}.t"

echo "--- async: failed dedup token remains retryable after schema drift"
${CLICKHOUSE_CLIENT} -q "SELECT code, payload FROM t ORDER BY payload"

# Retry the previously-failed entry with the same token and a value that fits — must succeed.
${CLICKHOUSE_CURL} -sS \
    -H 'X-Code: ab' \
    "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&insert_deduplication_token=token-invalid&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Code=code" \
    -d '{"payload":"dedup-retry"}'

${CLICKHOUSE_CLIENT} -q "SELECT code, payload FROM t ORDER BY payload"

# Test 3: String -> Array(String) (text-compatible, Field-incompatible) drift.
# Uses a separate table dropped at the end.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_tags;
    CREATE TABLE t_tags (tags String, payload String) ENGINE = MergeTree ORDER BY tuple();
"

${CLICKHOUSE_CURL} -sS \
    -H "X-Tags: ['a','b']" \
    "${CLICKHOUSE_URL}&${ASYNC_BUF}&query=INSERT+INTO+t_tags+(payload)+FORMAT+JSONEachRow&http_column_X-Tags=tags" \
    -d '{"payload":"text-compat"}'

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_tags MODIFY COLUMN tags Array(String)"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${CLICKHOUSE_DATABASE}.t_tags"

echo "--- async: text-compatible schema drift (String->Array(String)) succeeds via text re-parse"
${CLICKHOUSE_CLIENT} -q "SELECT tags, payload FROM t_tags"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_tags"

# Test 4: waiter with wait_for_async_insert=1 must receive an error, not silent success,
# when schema drift makes the buffered value unparseable at flush time.
# Recreate t with code String so ALTER to FixedString(1) succeeds on an empty
# table (String default '' fits FixedString(1); FixedString(N) default is N null
# bytes which would fail the metadata sample-block cast check for any N > 1).
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (code String, payload String) ENGINE = MergeTree ORDER BY tuple();
"

# Run ALTER+FLUSH in background: wait until the entry is queued, then shrink
# the column to invalidate the buffered value and trigger the flush.
{
    for _ in $(seq 1 100); do
        ${CLICKHOUSE_CLIENT} -q \
            "SELECT count() FROM system.asynchronous_inserts WHERE database=currentDatabase() AND table='t'" \
            2>/dev/null | grep -q '^[1-9]' && break
        sleep 0.1
    done
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE t MODIFY COLUMN code FixedString(1)"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${CLICKHOUSE_DATABASE}.t"
} &

# curl runs in the foreground; it blocks until the flush above resolves the wait.
drift_response=$(${CLICKHOUSE_CURL} -sS \
    -H 'X-Code: abcd' \
    "${CLICKHOUSE_URL}&${ASYNC_BUF_WAIT}&query=INSERT+INTO+t+(payload)+FORMAT+JSONEachRow&http_column_X-Code=code" \
    -d '{"payload":"drift-waiter"}')
wait

echo "--- async: schema drift with wait_for_async_insert=1, waiter receives error"
# Waiter must have received an error (TYPE_MISMATCH or similar).
echo "${drift_response}" | expect_match 'BAD_QUERY_PARAMETER|TOO_LARGE_STRING_SIZE'
# No rows must have been inserted.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

# --- FORMAT Native: body-or-header guard ---
# A column mapped via http_column_* must not be silently dropped from the Native
# body even when input_format_skip_unknown_fields=1. The insert must fail with
# INCORRECT_DATA so the header value cannot win without the caller noticing.
echo "--- sync: FORMAT Native body-column conflict is rejected"
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_native;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_native
        (event_type String, payload String)
        ENGINE = MergeTree ORDER BY tuple();
"
native_payload=$(${CLICKHOUSE_CLIENT} -q "SELECT 'body-value' AS event_type, 'p' AS payload FORMAT Native")
curl -sS \
    -H 'X-Event-Type: header-value' \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+${CLICKHOUSE_DATABASE}.t_native+(payload)+FORMAT+Native"\
"&http_column_X-Event-Type=event_type&input_format_skip_unknown_fields=1" \
    --data-binary "${native_payload}" 2>&1 | expect_match 'INCORRECT_DATA'
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.t_native"

# --- absent header is a hard error ---
# http_column_* mirrors param_*: if the referenced header is absent from the
# request, ClickHouse must reject it with BAD_QUERY_PARAMETER rather than
# silently inserting an empty/default value.
echo "--- sync: absent mapped header is rejected"
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_absent;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_absent
        (code UInt64, payload String)
        ENGINE = MergeTree ORDER BY tuple();
"
# No X-Code header sent — must error, not insert with default 0.
curl -sS \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+${CLICKHOUSE_DATABASE}.t_absent+(payload)+FORMAT+JSONEachRow"\
"&http_column_X-Code=code" \
    -d '{"payload":"p"}' 2>&1 | expect_match 'BAD_QUERY_PARAMETER'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_absent"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.t_absent"


#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./04666_run_query_in_background.lib
. "$CUR_DIR"/04666_run_query_in_background.lib

# A handler must stay invokable when the *request* lowers the parse limits. The invocation path parses the
# server's own stored query text with unlimited depth and backtracks and with a size limit that fits the text,
# and a request names settings freely - so `?max_parser_depth=...`, `?max_parser_backtracks=...` and
# `?max_query_size=...` must not lower those limits again for the handler's query.
# `04844_handler_deep_query_invocation` covers the same contract for a session-level limit.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique name and URL so parallel tests do not interfere (handlers are a global namespace).
HANDLER="h_parserlimits_${CLICKHOUSE_DATABASE}"
URL="/parserlimits_${CLICKHOUSE_DATABASE}"

# Nested deeper than the parser depth the requests below are limited to. The nesting is kept modest on
# purpose: a query deep enough to exceed the default limit of 1000 exhausts the thread stack of a sanitizer
# build before the depth limit is ever reached, which tests nothing about handlers.
DEEP="$(python3 -c "print('[' * 30 + '1' + ']' * 30)")"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${HANDLER}"

echo "=== created ==="
${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${HANDLER} URL '${URL}' AS SELECT length(${DEEP}[1]) AS len, {x:UInt64} AS p
" && echo "created"

echo "=== the request parse limits are in effect for an ordinary query ==="
${CLICKHOUSE_CURL} -sS "${BASE}/?max_parser_depth=10" -d "SELECT length(${DEEP}[1])" 2>&1 | grep -o -m1 "TOO_DEEP_RECURSION"
${CLICKHOUSE_CURL} -sS "${BASE}/?max_parser_backtracks=2" -d "SELECT length(${DEEP}[1])" 2>&1 | grep -o -m1 "TOO_SLOW_PARSING"
${CLICKHOUSE_CURL} -sS "${BASE}/?max_query_size=10" -d "SELECT length(${DEEP}[1])" 2>&1 | grep -o -m1 "Max query size exceeded"

echo "=== invoked over HTTP with the same request settings ==="
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?max_parser_depth=10&param_x=5"
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?max_parser_backtracks=2&param_x=6"
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?max_query_size=10&param_x=7"

echo "=== request-controlled construction settings still use the request limits ==="
# Only the server-owned stored text is parsed without limits. The `filter`/`select`/`order`/`page` snippets
# are named by the request, so they are parsed under the request's own parser limits.
${CLICKHOUSE_CURL} -sS "${BASE}${URL}?max_parser_depth=1&param_x=8&filter=(((((1)))))" 2>&1 | grep -o -m1 "TOO_DEEP_RECURSION"

echo "=== invoked through PARALLEL WITH under the same request limit ==="
PARALLEL_HANDLER="h_parallel_parserlimits_${CLICKHOUSE_DATABASE}"
PARALLEL_URL="/parallel_parserlimits_${CLICKHOUSE_DATABASE}"
PARALLEL_TABLE_1="parallel_parserlimits_1_${CLICKHOUSE_DATABASE}"
PARALLEL_TABLE_2="parallel_parserlimits_2_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${PARALLEL_HANDLER}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${PARALLEL_TABLE_1}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${PARALLEL_TABLE_2}"
${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${PARALLEL_HANDLER} URL '${PARALLEL_URL}' METHODS (POST) AS
    CREATE TABLE ${PARALLEL_TABLE_1} (x Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(Array(UInt8)))))))))))))))))))))))))))))))
    ENGINE = Memory
    PARALLEL WITH
    CREATE TABLE ${PARALLEL_TABLE_2} (x UInt8) ENGINE = Memory
"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${PARALLEL_URL}?database=${CLICKHOUSE_DATABASE}&max_parser_depth=10"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${PARALLEL_TABLE_1}"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${PARALLEL_TABLE_2}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${PARALLEL_TABLE_1}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${PARALLEL_TABLE_2}"
${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${PARALLEL_HANDLER}"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${HANDLER}"

echo "=== invoked in the background under the same request limit ==="
# `run_query_in_background` re-parses the same stored text on a background worker, with a copy of the
# invoking query context. The parse mode has to survive that copy, or a handler would become uninvokable
# in the background exactly when the request lowers the limits.
BACKGROUND_HANDLER="h_background_parserlimits_${CLICKHOUSE_DATABASE}"
BACKGROUND_URL="/background_parserlimits_${CLICKHOUSE_DATABASE}"
BACKGROUND_TABLE="background_parserlimits_${CLICKHOUSE_DATABASE}"
BACKGROUND_QUERY_ID="background_parserlimits_query_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${BACKGROUND_HANDLER}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${BACKGROUND_TABLE} (len UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${BACKGROUND_HANDLER} URL '${BACKGROUND_URL}' METHODS (POST) AS
    INSERT INTO ${BACKGROUND_TABLE} SELECT length(${DEEP}[1])
"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${BACKGROUND_URL}?database=${CLICKHOUSE_DATABASE}&run_query_in_background=1&max_parser_depth=10&query_id=${BACKGROUND_QUERY_ID}"
wait_for_query_log "$(finished_in_query_log "${BACKGROUND_QUERY_ID}")"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${BACKGROUND_TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${BACKGROUND_TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${BACKGROUND_HANDLER}"

echo "=== invoked with a remote fan-out under the same request limit ==="
# The shard receives the initiator's AST formatted back to SQL, and parses it under the settings the
# initiator sent - including the request's `max_parser_depth`. A handler query nested deeper than that
# must still run on the shard.
REMOTE_HANDLER="h_remote_parserlimits_${CLICKHOUSE_DATABASE}"
REMOTE_URL="/remote_parserlimits_${CLICKHOUSE_DATABASE}"
# Nested function calls rather than a nested literal: a constant expression would be folded away before
# the secondary query is formatted, and the shard would never see the deep text.
NESTED="$(python3 -c "print('identity(' * 30 + 'dummy' + ')' * 30)")"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${REMOTE_HANDLER}"
${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${REMOTE_HANDLER} URL '${REMOTE_URL}' AS SELECT ${NESTED} AS d FROM remote('127.0.0.2', system, one)
"
${CLICKHOUSE_CURL} -sS "${BASE}${REMOTE_URL}?max_parser_depth=10"
${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${REMOTE_HANDLER}"

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The query-construction settings (`select`/`filter`/`order`/`sort`) wrap the base query as a
# derived table. The wrapping is composed on the AST (in `executeQuery`), so it is robust against
# base queries that a text-level wrap would mangle: a trailing semicolon, a trailing comment, and a
# top-level `FORMAT` clause (which must be relocated to the outer query, not left inside the derived
# table). Exercised here end-to-end through the HTTP interface.

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

echo "--- trailing semicolon in base query ---"
${CLICKHOUSE_CURL} -sS "${BASE_URL}/?query=SELECT%20number%20FROM%20numbers(5)%3B&filter=number%3E2"

echo "--- trailing line comment in base query ---"
${CLICKHOUSE_CURL} -sS "${BASE_URL}/?query=SELECT%20number%20FROM%20numbers(5)%20--%20trailing%0A&filter=number%3E3"

echo "--- top-level FORMAT clause is relocated to the outer query ---"
${CLICKHOUSE_CURL} -sS "${BASE_URL}/?query=SELECT%20number%20FROM%20numbers(5)%20FORMAT%20JSONEachRow&filter=number%3E2"

echo "--- explicit SELECT list + ORDER BY composed on the AST ---"
${CLICKHOUSE_CURL} -sS "${BASE_URL}/?query=SELECT%20number,%20number*10%20AS%20x%20FROM%20numbers(5)&select=x&order=x%20DESC"

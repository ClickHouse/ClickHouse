#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A handler query is the server's own stored text and is parsed without the parse limits of the invoking
# request. An `ON CLUSTER` query it issues is executed from the DDL queue on every host: the entry carries
# the initiator's lifted `max_parser_depth` / `max_parser_backtracks` (zeroed on the query context, so they
# travel with the entry settings), and every host parses the entry text under them, clamped to its own
# constraints - or the handler becomes uninvokable as soon as the request names a low `max_parser_depth`.
# `04897_handler_request_parser_limits` covers the local, background and remote fan-out paths.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique name and URL so parallel tests do not interfere (handlers are a global namespace).
HANDLER="h_on_cluster_parserlimits_${CLICKHOUSE_DATABASE}"
URL="/on_cluster_parserlimits_${CLICKHOUSE_DATABASE}"
TABLE="on_cluster_parserlimits_${CLICKHOUSE_DATABASE}"

# Nested deeper than the parse depth the request below allows.
DEEP_TYPE="$(python3 -c "print('Array(' * 30 + 'UInt8' + ')' * 30)")"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${HANDLER}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"

${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${HANDLER} URL '${URL}' METHODS (POST) AS
    CREATE TABLE ${TABLE} ON CLUSTER test_shard_localhost (x ${DEEP_TYPE}) ENGINE = Memory
" && echo "created"

${CLICKHOUSE_CURL} -sS -X POST "${BASE}${URL}?database=${CLICKHOUSE_DATABASE}&max_parser_depth=10" > /dev/null
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${HANDLER}"

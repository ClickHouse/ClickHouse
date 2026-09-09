#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The stored text of a handler belongs to the server, and that has to keep holding one hop away from the
# node the request reached. Both cross-node paths ship SQL the server formatted, not text the caller typed:
#   * a `remote()` / `Distributed` fan-out sends `formatWithSecretsOneLine` output to the shard, and
#     `prepareSecondaryQuerySettings` pins `dialect = 'clickhouse'` in the settings that travel with it;
#   * an `ON CLUSTER` query is replayed from the DDL queue, and `DDLTaskBase::makeQueryContext` resets both
#     `dialect` and `max_query_size` on the replay context.
# Without those, `?dialect=clickhouse_json` or `?max_query_size=10` would still break a handler whose stored
# query fans out, even though the initiator itself parses it fine.
# `05052_handler_request_dialect` covers the initiator-local half of the `dialect` contract, and
# `05029_handler_on_cluster_parser_limits` the `ON CLUSTER` half of the parser-depth contract.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# Per-test-unique names and URLs so parallel tests do not interfere (handlers are a global namespace).
REMOTE_HANDLER="h_remote_dialect_${CLICKHOUSE_DATABASE}"
REMOTE_URL="/remote_dialect_${CLICKHOUSE_DATABASE}"
CLUSTER_HANDLER="h_cluster_dialect_${CLICKHOUSE_DATABASE}"
CLUSTER_URL="/cluster_dialect_${CLICKHOUSE_DATABASE}"
SIZE_HANDLER="h_cluster_query_size_${CLICKHOUSE_DATABASE}"
SIZE_URL="/cluster_query_size_${CLICKHOUSE_DATABASE}"
CLUSTER_TABLE="cluster_dialect_${CLICKHOUSE_DATABASE}"
SIZE_TABLE="cluster_query_size_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${REMOTE_HANDLER}"
${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${CLUSTER_HANDLER}"
${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${SIZE_HANDLER}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLUSTER_TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${SIZE_TABLE}"

echo "=== a handler reading over remote() keeps its dialect on the shard ==="
${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${REMOTE_HANDLER} URL '${REMOTE_URL}' AS
    SELECT dummy FROM remote('127.0.0.2', system.one)
" && echo "created"
${CLICKHOUSE_CURL} -sS "${BASE}${REMOTE_URL}?dialect=prql"
${CLICKHOUSE_CURL} -sS "${BASE}${REMOTE_URL}?dialect=kusto"
${CLICKHOUSE_CURL} -sS "${BASE}${REMOTE_URL}?dialect=clickhouse_json&enable_json_ast_dialect=1"

echo "=== a handler issuing ON CLUSTER keeps its dialect in the DDL queue ==="
${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${CLUSTER_HANDLER} URL '${CLUSTER_URL}' METHODS (POST) AS
    CREATE TABLE ${CLUSTER_TABLE} ON CLUSTER test_shard_localhost (x UInt8) ENGINE = Memory
" && echo "created"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${CLUSTER_URL}?database=${CLICKHOUSE_DATABASE}&dialect=clickhouse_json&enable_json_ast_dialect=1" > /dev/null
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${CLUSTER_TABLE}"

echo "=== a handler issuing ON CLUSTER is not bounded by the request max_query_size ==="
# The DDL entry text is the formatted AST plus a `/* ddl_entry=... */ ` prefix, so it is longer than the
# stored text and has nothing to do with the size of the request that invoked the handler.
${CLICKHOUSE_CLIENT} --query "
    CREATE HANDLER ${SIZE_HANDLER} URL '${SIZE_URL}' METHODS (POST) AS
    CREATE TABLE ${SIZE_TABLE} ON CLUSTER test_shard_localhost (x UInt8) ENGINE = Memory
" && echo "created"
${CLICKHOUSE_CURL} -sS -X POST "${BASE}${SIZE_URL}?database=${CLICKHOUSE_DATABASE}&max_query_size=10" > /dev/null
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${SIZE_TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${CLUSTER_TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${SIZE_TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${REMOTE_HANDLER}"
${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${CLUSTER_HANDLER}"
${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${SIZE_HANDLER}"

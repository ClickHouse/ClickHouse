#!/usr/bin/env bash
# Tags: no-replicated-database, no-fasttest
# no-replicated-database: the local ATTACH VIEW below is rejected in a Replicated database.
# no-fasttest: the clickhouse_json dialect is experimental.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE $db.t (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.t VALUES (1);
CREATE VIEW $db.v AS SELECT k FROM $db.t;
DETACH VIEW $db.v;
"

# An ASTStorage without an engine is short syntax just like a missing one, so the JSON AST
# dialect can express a shape the SQL parser never produces.
json=$(${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON('ATTACH VIEW $db.v ON CLUSTER test_shard_localhost')" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); d["storage"]={"type":"Storage"}; print(json.dumps(d))')

echo "$json" | ${CLICKHOUSE_CLIENT} --dialect clickhouse_json --enable_json_ast_dialect 1 \
    --distributed_ddl_task_timeout 10 2>&1 \
    | grep -q "INCORRECT_QUERY" && echo "INCORRECT_QUERY" || echo "NO ERROR"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '$db' AND name = 'v'"

# A columns node that declares nothing is short syntax too, and it formats back to a query without
# it, so the initiator would otherwise queue text that reparses as a short ATTACH on the workers.
json=$(${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON('ATTACH VIEW $db.v ON CLUSTER test_shard_localhost')" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); d["columns_list"]={"type":"Columns definition"}; print(json.dumps(d))')

echo "$json" | ${CLICKHOUSE_CLIENT} --dialect clickhouse_json --enable_json_ast_dialect 1 \
    --distributed_ddl_task_timeout 10 2>&1 \
    | grep -q "INCORRECT_QUERY" && echo "INCORRECT_QUERY" || echo "NO ERROR"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '$db' AND name = 'v'"

# `attach_as_replicated` is a table-only flag, and the conversion it asks for rewrites the metadata
# on disk. A view spelling aimed at a stored table must be rejected before that happens.
${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE $db.t2 (k UInt64) ENGINE = MergeTree ORDER BY k;
DETACH TABLE $db.t2;
"
json=$(${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON('ATTACH VIEW $db.t2')" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); d["attach_as_replicated"]=True; print(json.dumps(d))')

echo "$json" | ${CLICKHOUSE_CLIENT} --dialect clickhouse_json --enable_json_ast_dialect 1 2>&1 \
    | grep -q "INCORRECT_QUERY" && echo "INCORRECT_QUERY" || echo "NO ERROR"
${CLICKHOUSE_CLIENT} --multiquery --query "
ATTACH TABLE $db.t2;
SELECT engine FROM system.tables WHERE database = '$db' AND name = 't2';
DROP TABLE $db.t2;
"

${CLICKHOUSE_CLIENT} --multiquery --query "
ATTACH VIEW $db.v;
SELECT * FROM $db.v;
DROP VIEW $db.v;
DROP TABLE $db.t;
"

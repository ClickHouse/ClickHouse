#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# no-ordinary-database: a full-definition ATTACH needs the UUID clause, which Ordinary databases reject.
# no-replicated-database: explicit UUIDs are not allowed in Replicated databases by default.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A fresh full-definition ATTACH is user input and must be rejected like CREATE; a short ATTACH replays stored
# metadata and must keep loading definitions written before the guard existed.

UUID=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
DEFINITION="WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() AS cnt FROM c AS a, c AS b"

echo "-- fresh full-definition ATTACH is rejected with the guard on, whatever enable_materialized_cte is"
${CLICKHOUSE_CLIENT} --enable_materialized_cte 1 -q "ATTACH VIEW v_05142 UUID '$UUID' (cnt UInt64) AS $DEFINITION" 2>&1 | grep -oF 'SUPPORT_IS_DISABLED' | head -n 1
${CLICKHOUSE_CLIENT} --enable_materialized_cte 0 -q "ATTACH VIEW v_05142 UUID '$UUID' (cnt UInt64) AS $DEFINITION" 2>&1 | grep -oF 'SUPPORT_IS_DISABLED' | head -n 1

echo "-- with the guard off it is accepted"
# The server warns that a full-definition ATTACH is not recommended; keep that out of stderr.
${CLICKHOUSE_CLIENT} --force_materialized_cte 0 --send_logs_level fatal -q "ATTACH VIEW v_05142 UUID '$UUID' (cnt UInt64) AS $DEFINITION"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM v_05142 SETTINGS force_materialized_cte = 0, send_logs_level = 'fatal'"

echo "-- a short ATTACH replays the stored metadata with the guard on and materialization off"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE v_05142"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE v_05142"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM v_05142" 2>&1 | grep -oF 'SUPPORT_IS_DISABLED' | head -n 1
${CLICKHOUSE_CLIENT} -q "SELECT * FROM v_05142 SETTINGS force_materialized_cte = 0, send_logs_level = 'fatal'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE v_05142"

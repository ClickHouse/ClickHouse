#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --async_insert=0 -q "DROP TABLE IF EXISTS t_ast_json_ret"
${CLICKHOUSE_CLIENT} --async_insert=0 -q "CREATE TABLE t_ast_json_ret (id UInt64) ENGINE = Memory"

echo "json ast source settings apply"
JSON_APPLY=$(${CLICKHOUSE_CLIENT} --async_insert=0 -q "
    SELECT parseQueryToJSON(
        'INSERT INTO t_ast_json_ret
         SELECT number + toUInt64(getSettingOrDefault(''custom_insert_source'', ''unset'') = ''x'') * 100
         FROM numbers(1)
         RETURNING (SELECT max(id) FROM t_ast_json_ret)
         SETTINGS custom_insert_source = ''x'''
    ) FORMAT TSVRaw")
${CLICKHOUSE_CLIENT} --async_insert=0 --dialect clickhouse_json --enable_json_ast_dialect 1 -q "$JSON_APPLY"

echo "json ast unsupported source settings rejected"
JSON_REJECT=$(${CLICKHOUSE_CLIENT} --async_insert=0 -q "
    SELECT parseQueryToJSON(
        'INSERT INTO t_ast_json_ret
         SELECT number FROM numbers(1)
         RETURNING (SELECT count() FROM t_ast_json_ret)
         SETTINGS max_execution_time = 1'
    ) FORMAT TSVRaw")
OUT=$(${CLICKHOUSE_CLIENT} --async_insert=0 --dialect clickhouse_json --enable_json_ast_dialect 1 -q "$JSON_REJECT" 2>&1 || true)
echo "$OUT" | grep -qm1 'NOT_IMPLEMENTED' && echo 'rejected' || echo 'NO_REJECT'

${CLICKHOUSE_CLIENT} --async_insert=0 -q "DROP TABLE t_ast_json_ret"

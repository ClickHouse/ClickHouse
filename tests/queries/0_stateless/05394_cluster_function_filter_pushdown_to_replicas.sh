#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
CLUSTER_FN="icebergLocalCluster('test_cluster_two_shards_localhost', '${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64, mode String, d Date) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 --query "
    INSERT INTO ${TABLE} SELECT number, ['AIR', 'MAIL', 'SHIP'][number % 3 + 1], toDate('2024-01-01') + number % 100 FROM numbers(30000)"

QUERY="
    SELECT count(), sum(t.id)
    FROM ${CLUSTER_FN} AS t, (SELECT number AS k FROM numbers(30000)) AS n
    WHERE t.id = n.k AND t.mode IN ('MAIL', 'SHIP') AND t.d < toDate('2024-01-11') AND t.id % 2 = 0"

query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"
${CLICKHOUSE_CLIENT} --query_id="${query_id}" --query "${QUERY}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM ${TABLE} WHERE mode IN ('MAIL', 'SHIP') AND d < toDate('2024-01-11') AND id % 2 = 0"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        count() > 0 AND countIf(Settings['cluster_table_function_pushed_down_filter'] != '') = count() AS all_secondary_queries_have_filter,
        sum(result_rows) < 30000 AS replicas_returned_fewer_rows
    FROM system.query_log
    WHERE initial_query_id = '${query_id}' AND type = 'QueryFinish' AND is_initial_query = 0 AND query LIKE '%${TABLE}%'
        AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND is_initial_query)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"

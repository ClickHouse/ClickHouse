#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="05306_iceberg_staleness/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32) ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

run_select()
{
    ${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms="$1" --log_comment="${CLICKHOUSE_TEST_UNIQUE_NAME}_$2" \
        --query "SELECT sum(c0) FROM icebergS3(s3_conn, filename = '${TABLE_PATH}')"
}

run_select 600000 warm
run_select 600000 stale
run_select 0 fresh

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', ''), ProfileEvents['S3ListObjects'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('${CLICKHOUSE_TEST_UNIQUE_NAME}_stale', '${CLICKHOUSE_TEST_UNIQUE_NAME}_fresh')
    ORDER BY event_time_microseconds
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"

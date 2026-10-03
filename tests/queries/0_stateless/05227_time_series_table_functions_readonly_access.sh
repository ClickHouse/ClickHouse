#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
table="ts_${CLICKHOUSE_TEST_UNIQUE_NAME}"
qualified_table="${CLICKHOUSE_DATABASE}.${table}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${qualified_table}"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1 --multiquery --query "
    CREATE TABLE ${qualified_table} ENGINE = TimeSeries;
    INSERT INTO ${qualified_table} (metric_name, tags, samples)
        VALUES ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1)]);
    CREATE USER ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user};
"

readonly_client=(
    ${CLICKHOUSE_CLIENT}
    --user "${user}"
    --allow_experimental_time_series_table 1
)

"${readonly_client[@]}" --query "SELECT 'samples', count() FROM timeSeriesSamples(${qualified_table})"
"${readonly_client[@]}" --query "SELECT 'tags', count() FROM timeSeriesTags(${qualified_table})"
"${readonly_client[@]}" --query "SELECT 'metrics', count() FROM timeSeriesMetrics(${qualified_table})"
"${readonly_client[@]}" --query "SELECT 'selector', count() FROM timeSeriesSelector(${qualified_table}, 'm', 100, 100)"
"${readonly_client[@]}" --query "SELECT 'query', count() FROM prometheusQuery(${qualified_table}, 'm', 100)"
"${readonly_client[@]}" --query "SELECT 'range query', count() FROM prometheusQueryRange(${qualified_table}, 'm', 100, 100, 1)"

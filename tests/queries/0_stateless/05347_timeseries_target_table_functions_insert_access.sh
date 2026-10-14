#!/usr/bin/env bash

# Writing through a target table function of a TimeSeries table needs INSERT on that table and on the target,
# like `INSERT INTO` the TimeSeries table, and does not need SELECT.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1 -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    CREATE TABLE ${db}.ts ENGINE = TimeSeries;
    GRANT SELECT ON ${db}.ts TO ${user};
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${user};
"

phase=0
function try_inserts()
{
    phase=$((phase + 1))
    echo "-- with ${1}:"
    for insert in \
        "timeSeriesSamples(${db}.ts) (timestamp, value) VALUES (${phase}, 1)" \
        "timeSeriesTags(${db}.ts) (metric_name, tags) VALUES ('m${phase}', {'job': 'api'})" \
        "timeSeriesTagsMinMax(${db}.ts) (metric_name, min_time, max_time) VALUES ('m${phase}', 1000, 2000)" \
        "timeSeriesMetricFamilies(${db}.ts) (metric_family, type) VALUES ('m${phase}', 'gauge')"
    do
        error=$(${CLICKHOUSE_CLIENT} --user "${user}" --async_insert 0 -q "INSERT INTO FUNCTION ${insert}" 2>&1 \
            | grep -m1 -oE "grant (SELECT|INSERT) ON ${db}\.(ts|\`[^\`]*\`)|\([A-Z_]+\)" \
            | sed -E "s/${db}/db/; s/\`\.inner_id\.([a-z]+)\.[^\`]*\`/<\1 target>/" | paste -sd ' ' -)
        echo "${insert%%(*}: ${error:-OK}"
    done
    ${CLICKHOUSE_CLIENT} -q "
        SELECT (SELECT count() FROM timeSeriesSamples(${db}.ts)), (SELECT count() FROM timeSeriesTags(${db}.ts)),
               (SELECT count() FROM timeSeriesTagsMinMax(${db}.ts)), (SELECT count() FROM timeSeriesMetricFamilies(${db}.ts))
    "
}

try_inserts "SELECT"

${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${db}.ts TO ${user}"
try_inserts "SELECT, INSERT on the TimeSeries table"

${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${db}.* TO ${user}"
try_inserts "SELECT, INSERT on the TimeSeries table and its targets"

${CLICKHOUSE_CLIENT} -q "REVOKE SELECT ON ${db}.ts FROM ${user}"
try_inserts "INSERT on the TimeSeries table and its targets"

echo "-- reading still needs SELECT:"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT count() FROM timeSeriesTags(${db}.ts)" 2>&1 \
    | grep -m1 -oE "grant SELECT ON ${db}\.ts|\([A-Z_]+\)" | sed "s/${db}/db/" | paste -sd ' ' -

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE ${db}.ts;
    DROP USER ${user};
"

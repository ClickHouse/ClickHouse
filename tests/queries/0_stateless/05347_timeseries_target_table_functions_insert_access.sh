#!/usr/bin/env bash

# Writing through a target table function of a TimeSeries table needs INSERT on that table and on the target,
# like `INSERT INTO` the TimeSeries table, and does not need SELECT.
# Reading through it needs SELECT on that table and on the whole target.

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
        "timeSeriesSamples(${db}.ts) (id, bucket, samples) VALUES ((${phase}, '00000000-0000-0000-0000-000000000001'), 0, [(${phase}, 1)])" \
        "timeSeriesTags(${db}.ts) (metric_name, tags) VALUES ('m${phase}', {'job': 'api'})" \
        "timeSeriesTimeRanges(${db}.ts) (id, min_time, max_time) VALUES ((${phase}, '00000000-0000-0000-0000-000000000001'), 1000, 2000)" \
        "timeSeriesMetricFamilies(${db}.ts) (metric_family, type) VALUES ('m${phase}', 'gauge')"
    do
        error=$(${CLICKHOUSE_CLIENT} --user "${user}" --async_insert 0 -q "INSERT INTO FUNCTION ${insert}" 2>&1 \
            | grep -m1 -oE "grant (SELECT|INSERT) ON ${db}\.(ts|\`[^\`]*\`)|\([A-Z_]+\)" \
            | sed -E "s/${db}/db/; s/\`\.inner_id\.([a-z]+)\.[^\`]*\`/<\1 target>/" | paste -sd ' ' -)
        echo "${insert%%(*}: ${error:-OK}"
    done
    ${CLICKHOUSE_CLIENT} -q "
        SELECT (SELECT count() FROM timeSeriesSamples(${db}.ts)), (SELECT count() FROM timeSeriesTags(${db}.ts)),
               (SELECT count() FROM timeSeriesTimeRanges(${db}.ts)), (SELECT count() FROM timeSeriesMetricFamilies(${db}.ts))
    "
}

try_inserts "SELECT"

${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${db}.ts TO ${user}"
try_inserts "SELECT, INSERT on the TimeSeries table"

${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${db}.* TO ${user}"
try_inserts "SELECT, INSERT on the TimeSeries table and its targets"

${CLICKHOUSE_CLIENT} -q "REVOKE SELECT ON ${db}.ts FROM ${user}"
try_inserts "INSERT on the TimeSeries table and its targets"

function try_read()
{
    error=$(${CLICKHOUSE_CLIENT} --user "${user}" -q "${2}" 2>&1 \
        | grep -m1 -oE "grant (SELECT|SHOW COLUMNS) ON ${db}\.(\`[^\`]*\`|[a-z_]+)|\([A-Z_]+\)" \
        | sed -E "s/${db}/db/; s/\`\.inner_id\.([a-z]+)\.[^\`]*\`/<\1 target>/" | paste -sd ' ' -)
    echo "${1}: ${error:-OK}"
}

echo "-- reading needs SELECT on the TimeSeries table and on the whole target:"
try_read "without SELECT on the TimeSeries table" "SELECT count() FROM timeSeriesTags(${db}.ts)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.ts TO ${user}"
try_read "inner target without SELECT" "SELECT count() FROM timeSeriesTags(${db}.ts)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.* TO ${user}"
try_read "inner target with SELECT" "SELECT count() FROM timeSeriesTags(${db}.ts)"

# An external target is checked the same way, so a grant on the TimeSeries table does not open the columns of the target.
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1 -q "
    REVOKE ALL ON ${db}.* FROM ${user};
    CREATE TABLE ${db}.ext_tags (id UInt64, metric_name LowCardinality(String), tags Map(LowCardinality(String), String))
    ENGINE = ReplacingMergeTree ORDER BY (metric_name, id);
    CREATE TABLE ${db}.ts_ext ENGINE = TimeSeries SETTINGS version = 9 TAGS ${db}.ext_tags;
    GRANT SELECT ON ${db}.ts_ext TO ${user};
"
try_read "external target without grants" "SELECT count() FROM timeSeriesTags(${db}.ts_ext)"
try_read "external target without grants, DESCRIBE" "DESCRIBE timeSeriesTags(${db}.ts_ext)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT(id, metric_name) ON ${db}.ext_tags TO ${user}"
try_read "external target with SELECT on other columns" "SELECT tags FROM timeSeriesTags(${db}.ts_ext)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.ext_tags TO ${user}"
try_read "external target with SELECT" "SELECT tags FROM timeSeriesTags(${db}.ts_ext)"
try_read "external target with SELECT, DESCRIBE" "DESCRIBE timeSeriesTags(${db}.ts_ext)"

# The selector and the PromQL table functions need SELECT on the TimeSeries table to find it, even with SELECT on its targets only.
select_args="${db}.ts, 'm', 0, 1, 'samples'"
${CLICKHOUSE_CLIENT} -q "
    REVOKE ALL ON ${db}.* FROM ${user};
    GRANT SELECT ON ${db}.ts_ext TO ${user};
"
for target in $(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${db}' AND name LIKE '.inner_id.%'"); do
    ${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.\`${target}\` TO ${user}"
done
echo "-- the selector and the PromQL table functions need SELECT on the TimeSeries table:"
try_read "timeSeriesSelector with SELECT on the targets only" "DESCRIBE timeSeriesSelector(${select_args})"
try_read "prometheusQuery with SELECT on the targets only" "DESCRIBE prometheusQuery(${db}.ts, 'm', 0)"
try_read "prometheusQueryRange with SELECT on the targets only" "DESCRIBE prometheusQueryRange(${db}.ts, 'm', 0, 1, 1)"
try_read "timeSeriesSelector with SELECT on the targets only, reading" "SELECT count() FROM timeSeriesSelector(${select_args})"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.ts TO ${user}"
try_read "timeSeriesSelector with SELECT" "SELECT count() FROM timeSeriesSelector(${select_args})"
try_read "prometheusQuery with SELECT" "DESCRIBE prometheusQuery(${db}.ts, 'm', 0)"
try_read "prometheusQueryRange with SELECT" "DESCRIBE prometheusQueryRange(${db}.ts, 'm', 0, 1, 1)"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE ${db}.ts;
    DROP TABLE ${db}.ts_ext;
    DROP TABLE ${db}.ext_tags;
    DROP USER ${user};
"

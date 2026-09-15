#!/usr/bin/env bash

# A `TimeSeries` table accesses its target tables with the permissions given by its `SQL SECURITY` clause
# (by default those of the creating user), so reading or writing the table requires privileges on the table itself only.
# The table functions reading a `TimeSeries` table follow the same rule.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
reader="reader_${db}"
writer="writer_${db}"
outsider="outsider_${db}"
definer="definer_${db}"

# Runs a query as a user and prints its result, or the missing grant if the query is rejected.
# The database name, the UUIDs of inner tables and the column lists are replaced to keep the output stable.
function run()
{
    local user="$1" query="$2" output
    shift 2
    echo "-- ${user%%_*}: ${query}"
    output=$(${CLICKHOUSE_CLIENT} --user "${user}" "$@" -q "${query}" 2>&1 \
        | sed -E "s/${db}/db/g; s/[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}/<uuid>/g; s/(SELECT|INSERT)\([^)]*\)/\1(<columns>)/g")
    if [[ "${output}" == *"Not enough privileges"* ]]; then
        echo "${output}" | grep -oE "[a-z]+_db: Not enough privileges.*grant .* ON [^ ]+" | sed -E 's/\.$//' | head -n 1
    elif [[ -n "${output}" ]]; then
        echo "${output}"
    fi
}

function show_sql_security()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT extract(create_table_query, '(DEFINER = \\\\w+ )?SQL SECURITY \\\\w+') FROM system.tables WHERE database = currentDatabase() AND name = '$1'" \
        | sed "s/${db}/db/g"
}

${CLICKHOUSE_CLIENT} --enable_time_series_table 1 -q "
    DROP USER IF EXISTS ${reader}, ${writer}, ${outsider}, ${definer};
    CREATE USER ${reader}, ${writer}, ${outsider}, ${definer};

    CREATE TABLE ts ENGINE = TimeSeries;
    INSERT INTO ts (metric_name, tags, samples) VALUES ('http_requests', {'job': 'j1'}, [(now64(3), 1.)]);
    INSERT INTO ts (metric_family_name, type, unit, help) VALUES ('http_requests', 'counter', '', 'Requests');
    CREATE TABLE unrelated (x UInt8) ENGINE = Memory;

    GRANT SELECT ON ${db}.ts TO ${reader};
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${reader};
    GRANT INSERT ON ${db}.ts TO ${writer};
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${writer};
    GRANT SELECT ON ${db}.unrelated TO ${outsider};
"

echo "== The definition records the SQL security"
show_sql_security ts

echo "== SELECT ON ts is enough to read the table and its target tables"
run "${reader}" "SELECT count(metric_name) FROM ts"
run "${reader}" "SELECT count() FROM timeSeriesTags(ts)"
run "${reader}" "SELECT count() FROM timeSeriesSamples(ts)"
run "${reader}" "SELECT count() FROM timeSeriesMetrics(ts)"
run "${reader}" "SELECT count() FROM timeSeriesSelector(ts, 'http_requests', now() - INTERVAL 1 DAY, now() + INTERVAL 1 DAY)"
run "${reader}" "SELECT count() FROM prometheusQuery(ts, 'http_requests', now() + INTERVAL 1 MINUTE)"

echo "== The table functions work in the readonly mode"
run "${reader}" "SELECT count() FROM timeSeriesTags(ts)" --readonly 1

echo "== INSERT ON ts is enough to write the table and its target tables"
run "${writer}" "INSERT INTO ts (metric_name, tags, samples) VALUES ('http_requests', {'job': 'j2'}, [(now64(3), 2.)])"
run "${writer}" "INSERT INTO FUNCTION timeSeriesMetrics(ts) (metric_family_name, type, unit, help) VALUES ('http_errors', 'counter', '', 'Errors')"
run default "SELECT count() FROM timeSeriesTags(ts)"
run default "SELECT count() FROM timeSeriesMetrics(ts)"
run "${writer}" "SELECT count(metric_name) FROM ts"
run "${writer}" "SELECT count() FROM timeSeriesTags(ts)"

echo "== Without privileges on ts nothing can be read or written"
run "${outsider}" "SELECT count(metric_name) FROM ts"
run "${outsider}" "SELECT count() FROM timeSeriesTags(ts)"
run "${outsider}" "SELECT count() FROM timeSeriesSelector(ts, 'http_requests', now() - INTERVAL 1 DAY, now() + INTERVAL 1 DAY)"
run "${outsider}" "SELECT count() FROM prometheusQuery(ts, 'http_requests', now() + INTERVAL 1 MINUTE)"
run "${outsider}" "INSERT INTO ts (metric_name, tags, samples) VALUES ('http_requests', {'job': 'j3'}, [(now64(3), 3.)])"
run "${outsider}" "INSERT INTO FUNCTION timeSeriesMetrics(ts) (metric_family_name, type, unit, help) VALUES ('x', 'gauge', '', 'x')"

echo "== With SQL SECURITY INVOKER the privileges on the target tables are required"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ts MODIFY SQL SECURITY INVOKER"
show_sql_security ts
run "${reader}" "SELECT count(metric_name) FROM ts"
run "${reader}" "SELECT count() FROM timeSeriesTags(ts)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ts MODIFY DEFINER = default SQL SECURITY DEFINER"
show_sql_security ts
run "${reader}" "SELECT count(metric_name) FROM ts"
run "${reader}" "SELECT count() FROM timeSeriesTags(ts)"

echo "== The definer needs the privileges on external target tables, the reader doesn't"
${CLICKHOUSE_CLIENT} --enable_time_series_table 1 -q "
    CREATE TABLE ext_samples (id UUID, timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
    CREATE TABLE ext_tags (id UUID, metric_name LowCardinality(String), tags Map(LowCardinality(String), String)) ENGINE = MergeTree ORDER BY (metric_name, id);
    CREATE TABLE ext_metrics (metric_family_name String, type LowCardinality(String), unit LowCardinality(String), help String) ENGINE = MergeTree ORDER BY metric_family_name;
    CREATE TABLE ts_ext ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0
        SAMPLES ext_samples TAGS ext_tags METRICS ext_metrics
        DEFINER = ${definer} SQL SECURITY DEFINER;
    GRANT SELECT ON ${db}.ts_ext TO ${reader};
    GRANT SELECT ON ${db}.ext_samples TO ${definer};
    GRANT SELECT ON ${db}.ext_metrics TO ${definer};
"
show_sql_security ts_ext
run "${reader}" "SELECT count(metric_name) FROM ts_ext"
run "${reader}" "SELECT count() FROM timeSeriesTags(ts_ext)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.ext_tags TO ${definer}"
run "${reader}" "SELECT count(metric_name) FROM ts_ext"
run "${reader}" "SELECT count() FROM timeSeriesTags(ts_ext)"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE ts_ext;
    DROP TABLE ext_metrics;
    DROP TABLE ext_tags;
    DROP TABLE ext_samples;
    DROP TABLE ts;
    DROP TABLE unrelated;
    DROP USER ${reader}, ${writer}, ${outsider}, ${definer};
"

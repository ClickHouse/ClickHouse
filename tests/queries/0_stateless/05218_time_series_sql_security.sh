#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: the PromQL parser used by `timeSeriesSelector` and `prometheusQuery` requires the ANTLR4 grammars, which aren't built in the fast test.
# Tag no-parallel-replicas: every parallel replica reads all rows of the table behind `timeSeriesTags`, so each row
# is returned once per replica, see https://github.com/ClickHouse/ClickHouse/issues/118130.

# A `TimeSeries` table accesses its target tables with the permissions given by its `SQL SECURITY` clause
# (by default those of the creating user), so reading or writing the table requires privileges on the table itself only.
# The table functions `timeSeriesSelector` and `prometheusQuery` follow the same rule.
# The table functions `timeSeriesSamples`, `timeSeriesTags`, `timeSeriesTimeRanges` and `timeSeriesMetricFamilies` return a target table itself,
# so they require the privileges on that target table, the same as accessing it by its name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"

# `user` has privileges on the TimeSeries table only, `dbuser` on all the tables of the database (so on the inner tables too),
# `outsider` has no privileges, `definer` is the definer of a table which is read by `user`.
user="user_${db}"
dbuser="dbuser_${db}"
outsider="outsider_${db}"
definer="definer_${db}"

# Prints the `DEFINER` and `SQL SECURITY` clauses from the definitions of the TimeSeries tables of the current database.
show_sql_security="SELECT concat(name, ': ', extract(create_table_query, '(?:DEFINER = \\\\w+ )?SQL SECURITY \\\\w+')) FROM system.tables WHERE database = currentDatabase() AND name LIKE 'ts%' ORDER BY name"

# Prints the users and the missing grants from the errors of rejected queries. Only the exceptions received by the client
# are taken (the same text is also received in the server logs). The database name and the UUIDs of inner tables are
# replaced to keep the output stable.
function missing_grants()
{
    grep -E "^Code: " | sed -E "s/${db}/db/g; s/[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}/<uuid>/g" \
        | grep -oE "[a-z]+_db: Not enough privileges.*grant .* ON [^ ]+" | sed -E 's/ \(Missing permissions: [^)]*\)\.?$//; s/\.$//'
}

echo "== The definition records the SQL security, the default type comes from the default_time_series_sql_security setting"
${CLICKHOUSE_CLIENT} --enable_time_series_table 1 -q "
    DROP USER IF EXISTS ${user}, ${dbuser}, ${outsider}, ${definer};
    CREATE USER ${user}, ${dbuser}, ${outsider}, ${definer};

    CREATE TABLE ts ENGINE = TimeSeries;
    INSERT INTO ts (metric_name, tags, samples) VALUES ('http_requests', {'job': 'j1'}, [(now64(3), 1.)]);
    INSERT INTO ts (metric_family, type, unit, help) VALUES ('http_requests', 'counter', '', 'Requests');

    GRANT SELECT, INSERT ON ${db}.ts TO ${user};
    GRANT SELECT, INSERT ON ${db}.* TO ${dbuser};
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${user}, ${dbuser};

    SET default_time_series_sql_security = 'INVOKER';
    CREATE TABLE ts_invoker ENGINE = TimeSeries;
    ${show_sql_security};
    DROP TABLE ts_invoker;
"

echo "== Privileges on ts are enough to read and write the table and to use timeSeriesSelector and prometheusQuery"
${CLICKHOUSE_CLIENT} --user "${user}" --enable_time_series_table 1 -q "
    SELECT count(metric_name) FROM ts;
    SELECT count() FROM timeSeriesSelector(ts, 'http_requests', now() - INTERVAL 1 DAY, now() + INTERVAL 1 DAY);
    SELECT count() FROM prometheusQuery(ts, 'http_requests', now() + INTERVAL 1 MINUTE);
    INSERT INTO ts (metric_name, tags, samples) VALUES ('http_requests', {'job': 'j2'}, [(now64(3), 2.)]);
    SELECT count(metric_name) FROM ts;
"

echo "== The table functions returning a target table require the privileges on that target table"
${CLICKHOUSE_CLIENT} --user "${user}" --ignore-error -q "
    SELECT count() FROM timeSeriesTags(ts);
    SELECT count() FROM timeSeriesSamples(ts);
    INSERT INTO FUNCTION timeSeriesMetricFamilies(ts) (metric_family, type, unit, help) VALUES ('http_errors', 'counter', '', 'Errors');
" 2>&1 | missing_grants

echo "== A user with the privileges on the target tables can use the table functions, also in the readonly mode"
# The asynchronous insert queue doesn't keep the current database of the query, so the bare table name can't be resolved by its flush.
${CLICKHOUSE_CLIENT} --user "${dbuser}" --async_insert 0 -q "INSERT INTO FUNCTION timeSeriesMetricFamilies(ts) (metric_family, type, unit, help) VALUES ('http_errors', 'counter', '', 'Errors')"
${CLICKHOUSE_CLIENT} --user "${dbuser}" --readonly 1 --enable_time_series_table 1 -q "
    SELECT count() FROM timeSeriesTags(ts);
    SELECT count() FROM timeSeriesMetricFamilies(ts);
    SELECT count() FROM timeSeriesSelector(ts, 'http_requests', now() - INTERVAL 1 DAY, now() + INTERVAL 1 DAY);
    SELECT count() FROM prometheusQuery(ts, 'http_requests', now() + INTERVAL 1 MINUTE);
"

echo "== Without privileges on ts the table can't be read through the table functions and its target tables can't be found"
${CLICKHOUSE_CLIENT} --user "${outsider}" --ignore-error -q "
    SELECT count() FROM timeSeriesSelector(ts, 'http_requests', now() - INTERVAL 1 DAY, now() + INTERVAL 1 DAY);
    SELECT count() FROM prometheusQuery(ts, 'http_requests', now() + INTERVAL 1 MINUTE);
    SELECT count() FROM timeSeriesTags(ts);
" 2>&1 | missing_grants

echo "== Privileges on some of the columns of ts aren't enough for timeSeriesSelector and prometheusQuery"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT(metric_name) ON ${db}.ts TO ${outsider}"
${CLICKHOUSE_CLIENT} --user "${outsider}" --ignore-error -q "
    SELECT count() FROM timeSeriesSelector(ts, 'http_requests', now() - INTERVAL 1 DAY, now() + INTERVAL 1 DAY);
    SELECT count() FROM prometheusQuery(ts, 'http_requests', now() + INTERVAL 1 MINUTE);
" 2>&1 | missing_grants

echo "== With SQL SECURITY INVOKER the privileges on the target tables are required to read the table"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ts MODIFY SQL SECURITY INVOKER; ${show_sql_security}"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT count(metric_name) FROM ts" 2>&1 | missing_grants
${CLICKHOUSE_CLIENT} -q "ALTER TABLE ts MODIFY DEFINER = default SQL SECURITY DEFINER; ${show_sql_security}"

echo "== The definer's privileges on the target tables are checked when the table is read"
${CLICKHOUSE_CLIENT} --enable_time_series_table 1 -q "
    CREATE TABLE ts2 ENGINE = TimeSeries DEFINER = ${definer} SQL SECURITY DEFINER;
    GRANT SELECT ON ${db}.ts2 TO ${user};
    ${show_sql_security};
" | sed "s/${db}/db/g"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT count(metric_name) FROM ts2" 2>&1 | missing_grants
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.* TO ${definer}"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT count(metric_name) FROM ts2"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE ts2;
    DROP TABLE ts;
    DROP USER ${user}, ${dbuser}, ${outsider}, ${definer};
"

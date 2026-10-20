#!/usr/bin/env bash
# Tags: no-parallel-replicas
# Tag no-parallel-replicas: the rows read from the inner tables of a TimeSeries table can be returned once per replica,
# see https://github.com/ClickHouse/ClickHouse/issues/118130.

# A row policy of the user reading a `TimeSeries` table is applied to the tags the table returns, also when the query
# doesn't select the tag checked by the policy: the table reads its target tables with the permissions of the definer,
# but it keeps the tags needed by the policy of the reader.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user="user_${db}"

${CLICKHOUSE_CLIENT} --enable_time_series_table 1 -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    CREATE TABLE ts ENGINE = TimeSeries;
    INSERT INTO ts (metric_name, tags, samples) VALUES ('http_requests', {'job': 'a', 'env': 'secret'}, [(now64(3), 1.)]), ('http_requests', {'job': 'b', 'env': 'public'}, [(now64(3), 2.)]);
    GRANT SELECT ON ${db}.ts TO ${user};
    CREATE ROW POLICY hide_secret ON ${db}.ts USING tags['env'] != 'secret' TO ${user};
    CREATE ROW POLICY keep_all ON ${db}.ts USING 1 TO ALL EXCEPT ${user};
"

echo "-- the reader with the policy on the tag 'env' selects the tag 'job' only"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT tags['job'] FROM ts ORDER BY 1"
echo "-- the definer with a policy keeping all rows"
${CLICKHOUSE_CLIENT} -q "SELECT tags['job'] FROM ts ORDER BY 1"

${CLICKHOUSE_CLIENT} -q "
    DROP ROW POLICY hide_secret, keep_all ON ${db}.ts;
    DROP TABLE ts;
    DROP USER ${user};
"

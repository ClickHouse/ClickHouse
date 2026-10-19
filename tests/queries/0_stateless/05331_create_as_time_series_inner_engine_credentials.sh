#!/usr/bin/env bash

# a TimeSeries source holds its credentials in the inner engines, which the outer engine does not
# show. the copy inherits them, so it needs SELECT on the source.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"
client=(${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1)

# the URL is never contacted. only the definition is copied
"${client[@]}" -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${db}.* TO ${user};
    GRANT URL ON *.* TO ${user};
    GRANT TABLE ENGINE ON TimeSeries, TABLE ENGINE ON MergeTree, TABLE ENGINE ON AggregatingMergeTree,
          TABLE ENGINE ON ReplacingMergeTree, TABLE ENGINE ON URL TO ${user};

    CREATE TABLE ${db}.ts_src ENGINE = TimeSeries SETTINGS version = 8
        SAMPLES ENGINE = URL('http://user:password@127.0.0.1:1/', 'CSV');

    GRANT SHOW COLUMNS ON ${db}.ts_src TO ${user};
"

# prints the missing privilege, or the engine of the copy
function try_copy()
{
    echo "-- ${1}:"
    "${client[@]}" --user "${user}" -q "CREATE TABLE ${db}.${1} AS ${db}.ts_src SETTINGS version = 8" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = '${1}'"
}

echo "with SHOW COLUMNS only:"
try_copy copy_of_ts_src

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.ts_src TO ${user}"
try_copy copy_of_ts_src

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"

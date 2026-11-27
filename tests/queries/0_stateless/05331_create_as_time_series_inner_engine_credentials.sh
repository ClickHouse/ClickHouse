#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test does not build the encryption functions

# a TimeSeries source holds its credentials in the inner engines and inner columns, which the outer
# engine does not show. the copy inherits them, so it needs SELECT on the source.

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

    CREATE TABLE ${db}.ts_src ENGINE = TimeSeries
        METRIC FAMILIES ENGINE = URL('http://user:password@127.0.0.1:1/', 'CSV');
    CREATE TABLE ${db}.ts_columns_src ENGINE = TimeSeries
        TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(encrypt('aes-128-ecb', metric_name, '0123456789abcdef')));

    GRANT SHOW COLUMNS ON ${db}.ts_src TO ${user};
    GRANT SHOW COLUMNS ON ${db}.ts_columns_src TO ${user};
"

# prints the missing privilege, or the engine of the copy. the third argument goes to the query
function try_copy()
{
    echo "-- ${1}:"
    "${client[@]}" --user "${user}" -q "CREATE TABLE ${db}.${1} AS ${db}.${2} ${3:-}" 2>&1 \
        | grep -oE "necessary to have the grant [A-Z ]+ ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = '${1}'"
}

echo "with SHOW COLUMNS only:"
try_copy copy_of_ts_src ts_src
try_copy explicit_copy_of_ts_src ts_src "ENGINE = TimeSeries"
try_copy copy_of_ts_columns_src ts_columns_src
try_copy explicit_copy_of_ts_columns_src ts_columns_src "ENGINE = TimeSeries"

echo "after GRANT SELECT:"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.ts_src, SELECT ON ${db}.ts_columns_src TO ${user}"
try_copy copy_of_ts_src ts_src
try_copy explicit_copy_of_ts_src ts_src "ENGINE = TimeSeries"
try_copy copy_of_ts_columns_src ts_columns_src
try_copy explicit_copy_of_ts_columns_src ts_columns_src "ENGINE = TimeSeries"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"

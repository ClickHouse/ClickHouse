#!/usr/bin/env bash

# `CREATE TABLE ... AS <TimeSeries table>` reads the definition of the other table, so it requires the SHOW COLUMNS
# privilege on that table. The definition keeps all the types (the `id` type of an external tags table is recorded in
# the `id_type` setting), so the external target tables of the other table are not read and need no privileges.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

# All the target tables holding types are external here: `timestamp` is DateTime64(6) and `id` is UUID in them.
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1 -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    GRANT CREATE TABLE ON ${db}.* TO ${user};
    GRANT TABLE ENGINE ON TimeSeries, TABLE ENGINE ON MergeTree, TABLE ENGINE ON AggregatingMergeTree, TABLE ENGINE ON ReplacingMergeTree TO ${user};

    CREATE TABLE ${db}.ext_data (id UUID, timestamp DateTime64(6), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
    CREATE TABLE ${db}.ext_tags (id UUID, metric_name LowCardinality(String), tags Map(LowCardinality(String), String))
        ENGINE = MergeTree ORDER BY (metric_name, id);
    CREATE TABLE ${db}.ts_src ENGINE = TimeSeries
        SETTINGS store_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0
        DATA ${db}.ext_data TAGS ${db}.ext_tags;
"

create_copy="CREATE TABLE ${db}.ts_copy AS ${db}.ts_src ENGINE = TimeSeries
    DATA ENGINE = MergeTree ORDER BY (id, timestamp) TAGS ENGINE = AggregatingMergeTree ORDER BY (metric_name, id)"

# Prints the privileges of the user and either the error or the inner columns and the `id` settings of the created copy.
function try_create_copy()
{
    echo "-- with ${1}:"
    ${CLICKHOUSE_CLIENT} --user "${user}" --allow_experimental_time_series_table 1 -q "${create_copy}" 2>&1 \
        | grep -oE "Not enough privileges.*grant SHOW COLUMNS ON ${db}\.[a-z_]+" | head -n 1 | sed "s/${db}/db/"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \\((.*?)\\) SAMPLES INNER ENGINE'),
               extract(create_table_query, 'TAGS INNER COLUMNS \\((.*?)\\) TAGS INNER ENGINE'),
               extract(create_table_query, 'id_type = ''(.*?)'''),
               extract(create_table_query, 'id_generator = ''(.*?)''')
        FROM system.tables WHERE database = '${db}' AND name = 'ts_copy'
        FORMAT TSVRaw;
    "
}

# Without the privilege on the other table the copy is rejected.
try_create_copy "no SHOW COLUMNS privileges"

# The privilege on the other table is enough: the types are inherited from its definition, which records the `id` type
# and the expression generating identifiers of the external tags table in its settings.
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON ${db}.ts_src TO ${user}"
try_create_copy "SHOW COLUMNS ON ts_src"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE ${db}.ts_copy;
    DROP TABLE ${db}.ts_src;
    DROP TABLE ${db}.ext_tags;
    DROP TABLE ${db}.ext_data;
    DROP USER ${user};
"

#!/usr/bin/env bash
# `ABR` chooses the source table by inspecting the data of every candidate source table, and may retry the query on
# any of them. So reading an `ABR` table requires `SELECT` on all the candidate source tables, checked before
# anything about them is inspected or logged.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --allow_experimental_abr=1 --query "
    DROP USER IF EXISTS ${user};
    DROP TABLE IF EXISTS abr_proxy;
    DROP TABLE IF EXISTS src_1;
    DROP TABLE IF EXISTS src_10;

    CREATE TABLE src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    CREATE TABLE src_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    -- Interval 1 has data from 2026-01-10, interval 10 from 2026-01-01. The maximum value tells which table was read.
    INSERT INTO src_1 SELECT toDateTime('2026-01-10 00:00:00', 'UTC') + INTERVAL number DAY, 1 FROM numbers(10);
    INSERT INTO src_10 SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number DAY, 10 FROM numbers(20);

    CREATE TABLE abr_proxy ENGINE = ABR('src', [1, 10], ('ts'));

    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT ON abr_proxy TO ${user};
"

# Covered by interval 1, and only by interval 10.
query_fine="SELECT max(value) FROM abr_proxy WHERE ts >= toDateTime('2026-01-12 00:00:00', 'UTC')"
query_coarse="SELECT max(value) FROM abr_proxy WHERE ts >= toDateTime('2026-01-05 00:00:00', 'UTC')"

# Prints whether the query was denied, and how many of the logs returned to the user tell about the source tables.
function read_denied()
{
    local output
    output=$(${CLICKHOUSE_CLIENT} --user="${user}" --send_logs_level=debug "$@" 2>&1)
    echo "$(echo "${output}" | grep -o "ACCESS_DENIED" | uniq) $(echo "${output}" | grep -c -E "min_date|Selected table|falling back|Starting read")"
}

echo "SELECT without privileges on the source tables"
read_denied --query "${query_fine}"
read_denied --query "${query_coarse}"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON src_10 TO ${user}"
echo "SELECT with privileges on the coarse source table only"
read_denied --query "${query_fine}"
read_denied --query "${query_coarse}"

echo "SELECT limited to the coarse source table"
${CLICKHOUSE_CLIENT} --user="${user}" --abr_first_table_suffix=10 --query "${query_fine}"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON src_1 TO ${user}"
echo "SELECT with privileges on all the source tables"
${CLICKHOUSE_CLIENT} --user="${user}" --query "${query_fine}"
${CLICKHOUSE_CLIENT} --user="${user}" --query "${query_coarse}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE abr_proxy;
    DROP TABLE src_1;
    DROP TABLE src_10;
    DROP USER ${user};
"

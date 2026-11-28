#!/usr/bin/env bash
# A FileLog table parses its files with the format settings of its SETTINGS clause, which take precedence over the query's, and, in a direct SELECT, of the query.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

logs_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${logs_dir:?}"
mkdir -p "${logs_dir}"

# With `,` as the delimiter the line is read as 'x;y' and 'z'.
for f in table query mv; do printf 'x;y,z\n' > "${logs_dir}/${f}.csv"; done

${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_table (a String, b String) ENGINE = FileLog('${logs_dir}/table.csv', 'CSV')
    SETTINGS format_csv_delimiter = ';'"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE file_log_table"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE file_log_table"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a, b FROM file_log_table SETTINGS format_csv_delimiter = ','"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_query (a String, b String) ENGINE = FileLog('${logs_dir}/query.csv', 'CSV')"
${CLICKHOUSE_CLIENT} --stream_like_engine_allow_direct_select=1 -q "SELECT a, b FROM file_log_query SETTINGS format_csv_delimiter = ';'"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE file_log_mv (a String, b String) ENGINE = FileLog('${logs_dir}/mv.csv', 'CSV')
    SETTINGS format_csv_delimiter = ';', poll_directory_watch_events_backoff_init = 500, poll_directory_watch_events_backoff_max = 1000"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst (a String, b String) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW mv TO dst AS SELECT a, b FROM file_log_mv"
for _ in {1..240}; do
    [ "$(${CLICKHOUSE_CLIENT} -q 'SELECT count() FROM dst')" -ge 1 ] && break
    sleep 0.5
done
${CLICKHOUSE_CLIENT} -q "SELECT a, b FROM dst"

${CLICKHOUSE_CLIENT} -q "DROP TABLE mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_query"
${CLICKHOUSE_CLIENT} -q "DROP TABLE file_log_table"
rm -rf "${logs_dir:?}"

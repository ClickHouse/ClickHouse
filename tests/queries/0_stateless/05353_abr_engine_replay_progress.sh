#!/usr/bin/env bash

# The rows an `ABR` query reads are reported by the pipeline of the attempt that reads them. Replaying the buffered
# result must not report them again, or the query is counted twice in its read progress and in the `READ_ROWS` and
# `READ_BYTES` quotas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
quota="quota_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# The same rows must be read by the query on the source table and by the query on the `ABR` table.
SETTINGS="use_query_condition_cache = 0, enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --allow_experimental_abr=1 --query "
    DROP USER IF EXISTS ${user};
    DROP QUOTA IF EXISTS ${quota};
    DROP TABLE IF EXISTS abr;
    DROP TABLE IF EXISTS src_1;

    CREATE TABLE src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    INSERT INTO src_1 SELECT toDateTime('2026-01-01 00:00:00') + INTERVAL number MINUTE, number FROM numbers(100);
    CREATE TABLE abr ENGINE = ABR('src', [1], ('ts'));
"

echo "read_rows of the ABR query minus the one of the query on the source table"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(value) FROM src_1 WHERE ts >= toDateTime('2026-01-01 00:00:00')
    SETTINGS log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_direct', ${SETTINGS} FORMAT Null;
    SELECT sum(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00')
    SETTINGS log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_abr', ${SETTINGS} FORMAT Null;

    SELECT value % 2 AS k, count() FROM src_1 WHERE ts >= toDateTime('2026-01-01 00:00:00') GROUP BY k WITH TOTALS
    SETTINGS extremes = 1, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_direct_totals', ${SETTINGS} FORMAT Null;
    SELECT value % 2 AS k, count() FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') GROUP BY k WITH TOTALS
    SETTINGS extremes = 1, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_abr_totals', ${SETTINGS} FORMAT Null;

    SYSTEM FLUSH LOGS query_log;

    SELECT
        sumIf(read_rows, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_abr') - sumIf(read_rows, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_direct'),
        sumIf(read_rows, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_abr_totals') - sumIf(read_rows, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_direct_totals')
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
        AND log_comment LIKE '${CLICKHOUSE_TEST_UNIQUE_NAME}_%';
"

echo "The totals and extremes replayed from the attempt"
${CLICKHOUSE_CLIENT} --query "
    SELECT value % 2 AS k, count() FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') GROUP BY k WITH TOTALS ORDER BY k
    SETTINGS extremes = 1, ${SETTINGS} FORMAT TabSeparated;
"

# The query on the `ABR` table reads 100 rows, so a quota of 150 read rows lets it run once.
${CLICKHOUSE_CLIENT} --query "
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT ON src_1 TO ${user};
    GRANT SELECT ON abr TO ${user};
    CREATE QUOTA ${quota} FOR INTERVAL 1 HOUR MAX read_rows = 150 TO ${user};
"

echo "A query on the ABR table within the read_rows quota"
${CLICKHOUSE_CLIENT} --user="${user}" --query "
    SELECT sum(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') SETTINGS ${SETTINGS};
"

${CLICKHOUSE_CLIENT} --query "
    DROP QUOTA ${quota};
    DROP USER ${user};
    DROP TABLE abr;
    DROP TABLE src_1;
"

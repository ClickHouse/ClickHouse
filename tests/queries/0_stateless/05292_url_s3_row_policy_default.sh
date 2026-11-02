#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: uses S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A row policy on a `URL` or `S3` table must not remove its input columns from the header of the
# reading step. Before the fix, a `DEFAULT` column computed from the policy's input column failed
# with `UNKNOWN_IDENTIFIER`, with or without PREWHERE.

FILE="${CLICKHOUSE_DATABASE}_05292.parquet"

$CLICKHOUSE_CLIENT --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${FILE}', format = Parquet)
    SELECT number AS k, number % 10 AS a, concat('val_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS s3_truncate_on_insert = 1;
"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_url (k UInt64, a UInt64, s String, d UInt64 DEFAULT a * 2)
    ENGINE = URL('http://localhost:11111/test/${FILE}', Parquet);
    CREATE TABLE t_s3 (k UInt64, a UInt64, s String, d UInt64 DEFAULT a * 2)
    ENGINE = S3(s3_conn, filename = '${FILE}', format = Parquet);
    CREATE ROW POLICY policy_05292 ON t_url, t_s3 USING a != 0 TO ALL;
"

for table in t_url t_s3; do
    echo "-- ${table}"
    $CLICKHOUSE_CLIENT --query "
        SELECT k, d FROM ${table} ORDER BY k LIMIT 3;
        SELECT k, d FROM ${table} WHERE s != 'val_2' ORDER BY k LIMIT 3;
        SELECT k, s, d FROM ${table} WHERE a != 1 ORDER BY k LIMIT 3;
        SELECT k, s, d FROM ${table} WHERE s != 'val_2' ORDER BY k LIMIT 3
            SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 0;
        SELECT count(), sum(d) FROM ${table} WHERE s != 'val_2';
    "
done

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY policy_05292 ON t_url, t_s3;
    DROP TABLE t_url;
    DROP TABLE t_s3;
"

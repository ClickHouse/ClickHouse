#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# - no-fasttest: uses Iceberg
# - no-parallel-replicas: see the comment in `04071_iceberg_orc_prewhere_crash.sh`

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An Iceberg table with format `Parquet` and ORC data files: the source applies the row policy
# after the ORC reader, because ORC does not support PREWHERE. The row policy keeps its input
# columns in the block. If the filter column of the policy is an input itself (`USING a`), it must
# not be removed after filtering.

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05293"
rm -rf "${ICEBERG_PATH}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    SET input_format_parquet_use_native_reader_v3 = 1;
    CREATE TABLE t (k UInt64, a UInt64) ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');
    INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_PATH}', 'ORC', 'k UInt64, a UInt64')
        SELECT number, number % 3 FROM numbers(10);
    CREATE ROW POLICY policy_05293 ON t USING a TO ALL;
"

${CLICKHOUSE_CLIENT} --query "
    SET input_format_parquet_use_native_reader_v3 = 1;
    SELECT count() FROM t;
    SELECT k, a FROM t ORDER BY k;
    SELECT k FROM t WHERE k > 3 ORDER BY k;
    SELECT count() FROM t PREWHERE k > 3;
"

${CLICKHOUSE_CLIENT} --query "
    DROP ROW POLICY policy_05293 ON t;
    DROP TABLE t;
"
rm -rf "${ICEBERG_PATH}"

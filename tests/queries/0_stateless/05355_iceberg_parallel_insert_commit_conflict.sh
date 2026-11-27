#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: needs Iceberg (USE_AVRO)

# Each sink of a parallel INSERT commits its own snapshot, so the sinks race for the same metadata version
# and the losers retry. A lost race must not be logged as a warning or an error, which would reach stderr.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_DIR="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05355"
rm -rf "${ICEBERG_DIR}"

INSERT_SETTINGS="allow_experimental_insert_into_iceberg = 1, max_threads = 8, max_insert_threads = 8,
    max_block_size = 100, max_insert_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0"

# With the hint, every commit also advances `version-hint.text` with a conditional write.
for hint in 0 1; do
    ${CLICKHOUSE_CLIENT} --query "
        SET allow_experimental_insert_into_iceberg = 1;
        CREATE TABLE t_05355_${hint} (k Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}/${hint}', 'Parquet')
            SETTINGS iceberg_use_version_hint = ${hint};"
    for _ in {1..10}; do
        ${CLICKHOUSE_CLIENT} --query "
            INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_DIR}/${hint}', 'Parquet', 'k Int64')
            SELECT number FROM numbers(10000) SETTINGS ${INSERT_SETTINGS}"
    done
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), uniqExact(k), uniqExact(_path) > 10
        FROM icebergLocal('${ICEBERG_DIR}/${hint}', 'Parquet', 'k Int64')"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05355_${hint}"
done

rm -rf "${ICEBERG_DIR}"

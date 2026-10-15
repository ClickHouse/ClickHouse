#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/iceberg_colon_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
TABLE_DIR="${WORK_DIR}/t0:"
mkdir -p "${TABLE_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

${CLICKHOUSE_LOCAL} \
    --allow_insert_into_iceberg=1 \
    --multiquery -q "
CREATE TABLE t0 (c0 Int) ENGINE = IcebergLocal('${TABLE_DIR}/');
INSERT INTO t0 VALUES (42);
SELECT c0 FROM t0;
"

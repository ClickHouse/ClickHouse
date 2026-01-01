#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_NAME="t_${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/iceberg_bad_order_by_segv_93280"

mkdir -p "${TABLE_PATH}"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME} (c0 Int) ENGINE = IcebergLocal('${TABLE_PATH}') ORDER BY (1); -- { serverError BAD_ARGUMENTS }
"


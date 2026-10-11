#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WRITERS=16
DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -rf "${CLICKHOUSE_USER_FILES:?}/${DIR:?}"' EXIT
rm -rf "${CLICKHOUSE_USER_FILES:?}/${DIR:?}"

FILE="file('${DIR}/data.parquet', 'Parquet', 'x UInt64, y String')"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION ${FILE} VALUES (0, 'base')"

for i in $(seq 1 $WRITERS); do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION ${FILE}
        SETTINGS engine_file_allow_create_multiple_files = 1,
                 engine_file_truncate_on_insert = 0
        VALUES ($i, 'w$i')" &
done
wait

$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('${DIR}/*.parquet', 'Parquet', 'x UInt64, y String')"
$CLICKHOUSE_CLIENT -q "SELECT uniqExact(_file) FROM file('${DIR}/*.parquet', 'Parquet', 'x UInt64, y String')"

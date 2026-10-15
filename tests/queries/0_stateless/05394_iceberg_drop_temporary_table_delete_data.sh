#!/usr/bin/env bash
# Tags: no-fasttest
# Requires `IcebergLocal` (USE_AVRO).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_05394"

cleanup()
{
    rm -rf "${TABLE_PATH}_keep" "${TABLE_PATH}_purge"
}
trap cleanup EXIT

for DELETE_DATA in 0 1; do
    if [ "${DELETE_DATA}" = 1 ]; then SUFFIX=purge; else SUFFIX=keep; fi
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TEMPORARY TABLE t (x Int64) ENGINE = IcebergLocal('${TABLE_PATH}_${SUFFIX}/');
        INSERT INTO t SETTINGS allow_insert_into_iceberg = 1 VALUES (1);
        DROP TEMPORARY TABLE t SETTINGS data_lake_delete_data_on_drop = ${DELETE_DATA};
    "
    # The temporary table is dropped once the context of the `DROP` query is released, which can be after the client gets the result.
    if [ "${DELETE_DATA}" = 1 ]; then
        for _ in {1..600}; do
            [ -z "$(find "${TABLE_PATH}_${SUFFIX}" -type f 2>/dev/null)" ] && break
            sleep 0.1
        done
    fi
    echo "data_lake_delete_data_on_drop = ${DELETE_DATA}: $(find "${TABLE_PATH}_${SUFFIX}" -type f 2>/dev/null | wc -l | awk '{print ($1 > 0) ? "files kept" : "files deleted"}')"
done

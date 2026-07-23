#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="lance_local_insert_${CLICKHOUSE_TEST_UNIQUE_NAME//[^a-zA-Z0-9_]/_}"
TMP_DIR="${CUR_DIR}/tmp/${CLICKHOUSE_TEST_UNIQUE_NAME}"
DATASET="${TMP_DIR}/basic.lance"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_NAME}" >/dev/null 2>&1 || true
    rm -rf -- "${TMP_DIR:?}"
}
trap cleanup EXIT

mkdir -p "${TMP_DIR}"
cp -R "${CUR_DIR}/data_lance/basic.lance" "${DATASET}"

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE ${TABLE_NAME}
    ENGINE = LanceLocal('${DATASET}');

    INSERT INTO ${TABLE_NAME} VALUES (4, 'd', 40), (5, 'e', NULL);

    SELECT count(), sum(id), min(_data_lake_snapshot_version), max(_data_lake_snapshot_version)
    FROM ${TABLE_NAME};

    INSERT INTO ${TABLE_NAME} VALUES (6, 'f', 60);

    DETACH TABLE ${TABLE_NAME};
    ATTACH TABLE ${TABLE_NAME};

    SELECT id, name, score FROM ${TABLE_NAME} ORDER BY id;
    SELECT count(), sum(id), min(_data_lake_snapshot_version), max(_data_lake_snapshot_version)
    FROM ${TABLE_NAME};
"

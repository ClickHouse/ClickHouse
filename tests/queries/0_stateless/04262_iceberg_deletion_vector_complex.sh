#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_PATH="${CURDIR}/data_minio/dv_puffin_warehouse/default/dv_puffin_complex"

$CLICKHOUSE_LOCAL -q "
SELECT count(id) FROM icebergLocal('${TABLE_PATH}')
"

$CLICKHOUSE_LOCAL -q "
SELECT count(id) FROM icebergLocal('${TABLE_PATH}') WHERE label = 'new'
"

$CLICKHOUSE_LOCAL -q "
SELECT count(id) FROM icebergLocal('${TABLE_PATH}') WHERE label = 'updated'
"

$CLICKHOUSE_LOCAL -q "
SELECT label FROM icebergLocal('${TABLE_PATH}') WHERE id = 25
"

$CLICKHOUSE_LOCAL -q "
SELECT count(id) FROM icebergLocal('${TABLE_PATH}') WHERE label IS NULL
"

$CLICKHOUSE_LOCAL -q "
SELECT id FROM icebergLocal('${TABLE_PATH}') ORDER BY id
"

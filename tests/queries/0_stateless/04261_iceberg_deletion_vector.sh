#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_PATH="${CURDIR}/data_minio/dv_puffin_warehouse/default/dv_puffin_source"

$CLICKHOUSE_LOCAL -q "
SELECT count() FROM icebergLocal('${TABLE_PATH}')
"

$CLICKHOUSE_LOCAL -q "
SELECT id FROM icebergLocal('${TABLE_PATH}') ORDER BY id
"

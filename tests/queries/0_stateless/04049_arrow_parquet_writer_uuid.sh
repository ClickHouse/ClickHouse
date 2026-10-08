#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

$CLICKHOUSE_LOCAL -q "
    SELECT toUUID('4dfdc6b0-e3f2-4649-ad36-6598aff9c482') AS id
    INTO OUTFILE '${DATA_FILE}' FORMAT Parquet;
"

$CLICKHOUSE_LOCAL -q "
    SELECT toTypeName(id), id
    FROM file('${DATA_FILE}', Parquet);
"

rm -f "${DATA_FILE}"
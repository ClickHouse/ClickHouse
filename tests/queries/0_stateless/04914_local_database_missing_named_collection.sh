#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the S3 database engine, which requires ENABLE_LIBRARIES

# clickhouse-local keeps refusing to start: the startup tolerance is server-only.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

STORE="${CLICKHOUSE_TMP}/db_missing_nc_${CLICKHOUSE_DATABASE}"
rm -rf "${STORE}"

$CLICKHOUSE_LOCAL --path="${STORE}" --multiquery -q "
    CREATE NAMED COLLECTION nc_local AS
        url = 'http://localhost:11111/test/', access_key_id = 'k', secret_access_key = 's';
    CREATE DATABASE db_local ENGINE = S3(nc_local);
"

$CLICKHOUSE_LOCAL --path="${STORE}" -q "DROP NAMED COLLECTION nc_local"

$CLICKHOUSE_LOCAL --path="${STORE}" -q "SELECT 1" 2>&1 \
    | grep -o -m1 'NAMED_COLLECTION_DOESNT_EXIST'

rm -rf "${STORE}"

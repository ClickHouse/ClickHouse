#!/usr/bin/env bash

# Tags: no-fasttest
# no-fasttest: requires azureBlobStorage function

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONN_STR="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
CONTAINER="cont${CLICKHOUSE_DATABASE//_/}azurereadhist"

# Write a blob and read it back to populate per-connection metrics.
$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION
azureBlobStorage('${CONN_STR}', '${CONTAINER}', 'data.native', 'Native', 'auto', 'a UInt64')
SELECT * FROM numbers_mt(100000)
SETTINGS azure_truncate_on_insert = 1
"

$CLICKHOUSE_CLIENT -q "
SELECT * FROM azureBlobStorage('${CONN_STR}', '${CONTAINER}', 'data.native')
FORMAT Null
"

# Both metrics should have at least one observation in the +Inf bucket (cumulative total count).
$CLICKHOUSE_CLIENT -q "
SELECT value > 0
FROM system.histogram_metrics
WHERE name = 'azure_read_request_duration_microseconds'
  AND labels['le'] = '+Inf'
"

$CLICKHOUSE_CLIENT -q "
SELECT value > 0
FROM system.histogram_metrics
WHERE name = 'azure_read_request_bytes'
  AND labels['le'] = '+Inf'
"

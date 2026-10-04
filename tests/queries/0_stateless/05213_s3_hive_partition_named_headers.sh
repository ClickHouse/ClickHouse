#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: needs Minio

# Of the object storage configurations only `Web` reads an HTTP response, so it alone registers
# `_headers` as a response-header `Map`. On `s3`/`azureBlobStorage`/`hdfs` a `_headers` of that name
# is a Hive path key and must resolve to the path value.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_hive_headers"
AUTH="'test', 'testtest'"

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION s3('${URL}/_headers=abc/data.tsv', ${AUTH}, 'TSV', 'x UInt64')
SELECT 1 SETTINGS s3_truncate_on_insert = 1;
"

$CLICKHOUSE_CLIENT --use_hive_partitioning=1 -q "
SELECT _headers FROM s3('${URL}/_headers=abc/data.tsv', ${AUTH}, 'TSV', 'x UInt64');
"

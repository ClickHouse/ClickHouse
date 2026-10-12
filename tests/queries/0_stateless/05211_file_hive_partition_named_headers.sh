#!/usr/bin/env bash
# `_headers` is an HTTP response-header Map only for the sources that read a response (url, URL,
# urlCluster, Web object storage). For every other file-like storage a Hive path key of that name
# must still resolve to the path value.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR="hive_headers_$CLICKHOUSE_TEST_UNIQUE_NAME"

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$DIR/_headers=abc/data.tsv', 'TSV', 'x UInt64')
SETTINGS engine_file_truncate_on_insert = 1
SELECT 1;
"

$CLICKHOUSE_CLIENT --use_hive_partitioning=1 -q "
SELECT _headers FROM file('$DIR/_headers=abc/data.tsv', 'TSV', 'x UInt64');
-- The type pins that the Hive virtual resolves here, not a response-header Map.
SELECT toTypeName(_headers) FROM file('$DIR/_headers=abc/data.tsv', 'TSV', 'x UInt64');
"

DATA_FILE_PATH=$($CLICKHOUSE_CLIENT -q "SELECT _path FROM file('$DIR/_headers=abc/data.tsv', 'TSV', 'x UInt64') LIMIT 1")
rm -rf "$(dirname "$(dirname "$DATA_FILE_PATH")")"

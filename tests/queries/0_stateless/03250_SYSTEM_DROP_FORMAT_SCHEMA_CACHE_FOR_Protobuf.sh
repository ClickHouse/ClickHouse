#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMADIR=$CLICKHOUSE_SCHEMA_FILES
CLIENT_SCHEMADIR=$CURDIR/format_schemas
export SERVER_SCHEMADIR=$CLICKHOUSE_DATABASE
mkdir -p $SCHEMADIR/$SERVER_SCHEMADIR
cp -r $CLIENT_SCHEMADIR/03250.proto $SCHEMADIR/$SERVER_SCHEMADIR/

$CLICKHOUSE_CLIENT --query "SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf"

BINARY_FILE_PATH=$(mktemp "$CLICKHOUSE_USER_FILES/03250.XXXXXX.binary")
export BINARY_FILE_PATH
$CLICKHOUSE_CLIENT --query "SELECT * FROM numbers(10) FORMAT Protobuf SETTINGS format_schema = '$CLIENT_SCHEMADIR/03250:Numbers'" > $BINARY_FILE_PATH
chmod 666 "$BINARY_FILE_PATH"

function protobuf_reader()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$(basename $BINARY_FILE_PATH)', 'Protobuf') FORMAT Null SETTINGS max_threads=1, format_schema='$SERVER_SCHEMADIR/03250:Numbers'"
    done
}
export -f protobuf_reader

function protobuf_cache_drainer()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf"
    done
}
export -f protobuf_cache_drainer

timeout 20 bash -c protobuf_reader &
timeout 20 bash -c protobuf_cache_drainer &
wait

rm -f "${BINARY_FILE_PATH:?}"
rm -fr "${SCHEMADIR:?}/${SERVER_SCHEMADIR:?}/"

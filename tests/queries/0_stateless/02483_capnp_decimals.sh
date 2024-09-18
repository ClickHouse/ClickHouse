#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
touch $USER_FILES_PATH/data.capnp

SCHEMADIR=$($CLICKHOUSE_CLIENT_BINARY --query "select * from file('data.capnp', 'CapnProto', 'val1 char') settings format_schema='nonexist:Message'" 2>&1 | grep Exception | grep -oP "file \K.*(?=/nonexist.capnp)")
CLIENT_SCHEMADIR=$CURDIR/format_schemas
SERVER_SCHEMADIR=test_02483
mkdir -p $SCHEMADIR/$SERVER_SCHEMADIR
cp -r $CLIENT_SCHEMADIR/02483_* $SCHEMADIR/$SERVER_SCHEMADIR/


$CLICKHOUSE_CLIENT -q "insert into function file(02483_data.capnp, auto, 'decimal32 Decimal32(3), decimal64 Decimal64(6)') select 42.42, 4242.424242 settings format_schema='$SERVER_SCHEMADIR/02483_decimals.capnp:Message', engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02483_data.capnp) settings format_schema='$SERVER_SCHEMADIR/02483_decimals.capnp:Message'"
$CLICKHOUSE_CLIENT -q "select * from file(02483_data.capnp, auto, 'decimal64 Decimal64(6), decimal32 Decimal32(3)') settings format_schema='$SERVER_SCHEMADIR/02483_decimals.capnp:Message'"

rm $USER_FILES_PATH/data.capnp
rm $USER_FILES_PATH/02483_data.capnp

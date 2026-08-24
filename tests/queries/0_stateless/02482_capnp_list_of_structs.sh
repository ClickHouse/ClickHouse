#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

touch $USER_FILES_PATH/data.capnp

SCHEMADIR=${CLICKHOUSE_SCHEMA_FILES}
CLIENT_SCHEMADIR=$CURDIR/format_schemas
SERVER_SCHEMADIR=test_02482
mkdir -p $SCHEMADIR/$SERVER_SCHEMADIR
cp -r $CLIENT_SCHEMADIR/02482_* $SCHEMADIR/$SERVER_SCHEMADIR/


$CLICKHOUSE_CLIENT -q "insert into function file(02482_data.capnp, auto, 'nested Nested(x Int64, y Int64)') select [1,2], [3,4] settings format_schema='$SERVER_SCHEMADIR/02482_list_of_structs.capnp:Nested', engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02482_data.capnp) settings format_schema='$SERVER_SCHEMADIR/02482_list_of_structs.capnp:Nested'"
$CLICKHOUSE_CLIENT -q "select * from file(02482_data.capnp, auto, 'nested Nested(x Int64, y Int64)') settings format_schema='$SERVER_SCHEMADIR/02482_list_of_structs.capnp:Nested'"
$CLICKHOUSE_CLIENT -q "select * from file(02482_data.capnp, auto, '\`nested.x\` Array(Int64), \`nested.y\` Array(Int64)') settings format_schema='$SERVER_SCHEMADIR/02482_list_of_structs.capnp:Nested'"
$CLICKHOUSE_CLIENT -q "select * from file(02482_data.capnp, auto, '\`nested.x\` Array(Int64)') settings format_schema='$SERVER_SCHEMADIR/02482_list_of_structs.capnp:Nested'"

rm $USER_FILES_PATH/data.capnp
rm $USER_FILES_PATH/02482_data.capnp

#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME=test_02149.data
DATA_FILE=$USER_FILES_PATH/$FILE_NAME

touch $DATA_FILE

SCHEMADIR=$(clickhouse-client --query "select * from file('$FILE_NAME', 'CapnProto', 'val1 char') settings format_schema='nonexist:Message'" 2>&1 | grep Exception | grep -oP "file \K.*(?=/nonexist.capnp)")
CLIENT_SCHEMADIR=$CURDIR/format_schemas
SERVER_SCHEMADIR=test_02149
mkdir -p $SCHEMADIR/$SERVER_SCHEMADIR
cp -r $CLIENT_SCHEMADIR/* $SCHEMADIR/$SERVER_SCHEMADIR/

echo -e "Protobuf\n"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Protobuf') settings format_schema='$SERVER_SCHEMADIR/00825_protobuf_format_array_3dim:ABC'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Protobuf') settings format_schema='$SERVER_SCHEMADIR/00825_protobuf_format_array_of_arrays:AA'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Protobuf') settings format_schema='$SERVER_SCHEMADIR/00825_protobuf_format_enum_mapping.proto:EnumMessage'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Protobuf') settings format_schema='$SERVER_SCHEMADIR/00825_protobuf_format_map:Message'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Protobuf') settings format_schema='$SERVER_SCHEMADIR/00825_protobuf_format_nested_in_nested:MessageType'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Protobuf') settings format_schema='$SERVER_SCHEMADIR/00825_protobuf_format_persons:Person'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Protobuf') settings format_schema='$SERVER_SCHEMADIR/00825_protobuf_format_persons:AltPerson'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Protobuf') settings format_schema='$SERVER_SCHEMADIR/00825_protobuf_format_persons:StrPerson'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Protobuf') settings format_schema='$SERVER_SCHEMADIR/00825_protobuf_format_persons_syntax2:Syntax2Person'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Protobuf') settings format_schema='$SERVER_SCHEMADIR/00825_protobuf_format_skipped_column_in_nested:UpdateMessage'"


echo -e "\nCapnproto\n"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CapnProto') settings format_schema='$SERVER_SCHEMADIR/02030_capnp_enum:Message'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CapnProto') settings format_schema='$SERVER_SCHEMADIR/02030_capnp_lists:Message'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CapnProto') settings format_schema='$SERVER_SCHEMADIR/02030_capnp_low_cardinality:Message'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CapnProto') settings format_schema='$SERVER_SCHEMADIR/02030_capnp_nested_lists_and_tuples:Message'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CapnProto') settings format_schema='$SERVER_SCHEMADIR/02030_capnp_nested_table:Message'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CapnProto') settings format_schema='$SERVER_SCHEMADIR/02030_capnp_nested_tuples:Message'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CapnProto') settings format_schema='$SERVER_SCHEMADIR/02030_capnp_nullable:Message'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CapnProto') settings format_schema='$SERVER_SCHEMADIR/02030_capnp_simple_types:Message'"

echo
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'CapnProto') settings format_schema='$SERVER_SCHEMADIR/02030_capnp_tuples:Message'"

echo -e "\nRawBLOB\n"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'RawBLOB')"

echo -e "\nLineAsString\n"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'LineAsString')"

echo -e "\nJSONAsString\n"
$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'JSONAsString')"



rm -rf ${SCHEMADIR:?}/$SERVER_SCHEMADIR
rm $DATA_FILE

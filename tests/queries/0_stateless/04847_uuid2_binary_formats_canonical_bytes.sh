#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: MsgPack and CapnProto support are not compiled into the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The binary interchange representation of UUID2 is the canonical big-endian 16 bytes, the same bytes
# RowBinary and Native write. Pin that contract for the MsgPack `bin` representation and the CapnProto
# `Data` mapping: the emitted payload must be the canonical bytes an external producer or consumer of
# standard UUID payloads expects, not the raw storage layout of the historical UUID, and reading the
# canonical bytes back must restore the same value.

u="00112233-4455-6677-8899-aabbccddeeff"
canonical="00112233445566778899aabbccddeeff"

echo "-- MsgPack bin writes the canonical bytes"
$CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x SETTINGS output_format_msgpack_uuid_representation = 'bin' FORMAT MsgPack" \
    | xxd -p | tr -d '\n' | grep -oc "$canonical"

echo "-- MsgPack bin, ext and str round-trip"
for repr in bin ext str
do
    echo -n "$repr: "
    $CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x SETTINGS output_format_msgpack_uuid_representation = '$repr' FORMAT MsgPack" \
        | $CLICKHOUSE_LOCAL --input-format MsgPack --structure "x UUID2" -q "SELECT toTypeName(x), toString(x) FROM table"
done

echo "-- MsgPack ext bytes are identical for UUID and UUID2 (both canonical)"
ext_uuid=$($CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID AS x SETTINGS output_format_msgpack_uuid_representation = 'ext' FORMAT MsgPack" | xxd -p | tr -d '\n')
ext_uuid2=$($CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x SETTINGS output_format_msgpack_uuid_representation = 'ext' FORMAT MsgPack" | xxd -p | tr -d '\n')
[ "$ext_uuid" == "$ext_uuid2" ] && echo "1"

echo "-- BSONEachRow binary subtype 4 holds the canonical bytes"
$CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x FORMAT BSONEachRow" | xxd -p | tr -d '\n' | grep -oc "$canonical"

echo "-- BSONEachRow round-trip"
$CLICKHOUSE_LOCAL -q "SELECT '$u'::UUID2 AS x FORMAT BSONEachRow" \
    | $CLICKHOUSE_LOCAL --input-format BSONEachRow --structure "x UUID2" -q "SELECT toTypeName(x), toString(x) FROM table"

echo "-- CapnProto Data field holds the canonical bytes"
CLIENT_SCHEMADIR=$CUR_DIR/format_schemas
SERVER_SCHEMADIR=$CLICKHOUSE_SCHEMA_FILES/test_04847_$CLICKHOUSE_DATABASE
mkdir -p "$SERVER_SCHEMADIR"
cp "$CLIENT_SCHEMADIR"/04847_uuid2.capnp "$SERVER_SCHEMADIR"/
DATA_FILE=04847_uuid2_${CLICKHOUSE_DATABASE}.capnp

$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$DATA_FILE', 'CapnProto', 'u UUID2') SELECT '$u'::UUID2 SETTINGS format_schema = 'test_04847_$CLICKHOUSE_DATABASE/04847_uuid2.capnp:Message', engine_file_truncate_on_insert = 1"
xxd -p "$CLICKHOUSE_USER_FILES/$DATA_FILE" | tr -d '\n' | grep -oc "$canonical"

echo "-- CapnProto round-trip"
$CLICKHOUSE_CLIENT -q "SELECT toTypeName(u), toString(u) FROM file('$DATA_FILE', 'CapnProto', 'u UUID2') SETTINGS format_schema = 'test_04847_$CLICKHOUSE_DATABASE/04847_uuid2.capnp:Message'"

rm "$CLICKHOUSE_USER_FILES/$DATA_FILE"
rm -r "$SERVER_SCHEMADIR"

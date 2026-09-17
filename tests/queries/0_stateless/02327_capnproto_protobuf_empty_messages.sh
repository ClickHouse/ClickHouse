#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-ordinary-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

touch $CLICKHOUSE_USER_FILES_UNIQUE/data.capnp

SCHEMADIR=${CLICKHOUSE_SCHEMA_FILES}
CLIENT_SCHEMADIR=$CURDIR/format_schemas
SERVER_SCHEMADIR=test_02327
mkdir -p $SCHEMADIR/$SERVER_SCHEMADIR
cp -r $CLIENT_SCHEMADIR/02327_* $SCHEMADIR/$SERVER_SCHEMADIR/


$CLICKHOUSE_CLIENT --query="desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data.pb') settings format_schema='$SERVER_SCHEMADIR/02327_schema:MessageWithEmpty'" 2>&1 | grep -F -q 'The table structure cannot be extracted' && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data.capnp') settings format_schema='$SERVER_SCHEMADIR/02327_schema:MessageWithEmpty'" 2>&1 | grep -F -q 'The table structure cannot be extracted' && echo 'OK' || echo 'FAIL';

$CLICKHOUSE_CLIENT --query="create table test_protobuf engine=File(Protobuf) settings format_schema='$SERVER_SCHEMADIR/02327_schema:MessageWithEmpty'" 2>&1 | grep -F -q 'The table structure cannot be extracted' && echo 'OK' || echo 'FAIL';
$CLICKHOUSE_CLIENT --query="create table test_capnp engine=File(CapnProto) settings format_schema='$SERVER_SCHEMADIR/02327_schema:MessageWithEmpty'" 2>&1 | grep -F -q 'The table structure cannot be extracted' && echo 'OK' || echo 'FAIL';

$CLICKHOUSE_CLIENT --query="desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data.pb') settings format_schema='$SERVER_SCHEMADIR/02327_schema:MessageWithEmpty', input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference=1";
$CLICKHOUSE_CLIENT --query="desc file('${CLICKHOUSE_TEST_UNIQUE_NAME}/data.capnp') settings format_schema='$SERVER_SCHEMADIR/02327_schema:MessageWithEmpty', input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference=1";

$CLICKHOUSE_CLIENT --query="drop table if exists test_protobuf";
$CLICKHOUSE_CLIENT --query="create table test_protobuf engine=File(Protobuf) settings format_schema='$SERVER_SCHEMADIR/02327_schema:MessageWithEmpty', input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference=1";
$CLICKHOUSE_CLIENT --query="desc test_protobuf";
$CLICKHOUSE_CLIENT --query="drop table test_protobuf";

$CLICKHOUSE_CLIENT --query="drop table if exists test_capnp";
$CLICKHOUSE_CLIENT --query="create table test_capnp engine=File(CapnProto) settings format_schema='$SERVER_SCHEMADIR/02327_schema:MessageWithEmpty', input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference=1";
$CLICKHOUSE_CLIENT --query="desc test_capnp";
$CLICKHOUSE_CLIENT --query="drop table test_capnp";

rm -rf ${SCHEMADIR:?}/${SERVER_SCHEMADIR:?}

#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p $USER_FILES_PATH/test_02402
cp $CURDIR/data_capnp/overflow.capnp $USER_FILES_PATH/test_02402/

SCHEMADIR=${CLICKHOUSE_SCHEMA_FILES}

CLIENT_SCHEMADIR=$CURDIR/format_schemas
SERVER_SCHEMADIR=test_02402

mkdir -p $SCHEMADIR/$SERVER_SCHEMADIR
cp -r $CLIENT_SCHEMADIR/02402_* $SCHEMADIR/$SERVER_SCHEMADIR/

$CLICKHOUSE_CLIENT --query="SELECT * FROM file('test_02402/overflow.capnp', 'CapnProto') SETTINGS format_schema='$SERVER_SCHEMADIR/02402_overflow:CapnProto'" 2>&1 | grep -F -q "INCORRECT_DATA" && echo 'OK' || echo 'FAIL';

rm -rf $USER_FILES_PATH/test_02402
rm -rf ${SCHEMADIR:?}/${SERVER_SCHEMADIR:?}

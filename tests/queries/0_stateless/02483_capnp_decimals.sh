#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

touch $CLICKHOUSE_USER_FILES_UNIQUE/data.capnp

CLIENT_SCHEMADIR=$CURDIR/format_schemas
SERVER_SCHEMADIR=test_02483
mkdir -p $CLICKHOUSE_SCHEMA_FILES/$SERVER_SCHEMADIR
cp -r $CLIENT_SCHEMADIR/02483_* $CLICKHOUSE_SCHEMA_FILES/$SERVER_SCHEMADIR/


$CLICKHOUSE_CLIENT -q "insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}/02483_data.capnp', auto, 'decimal32 Decimal32(3), decimal64 Decimal64(6)') select 42.42, 4242.424242 settings format_schema='$SERVER_SCHEMADIR/02483_decimals.capnp:Message', engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/02483_data.capnp') settings format_schema='$SERVER_SCHEMADIR/02483_decimals.capnp:Message'"
$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/02483_data.capnp', auto, 'decimal64 Decimal64(6), decimal32 Decimal32(3)') settings format_schema='$SERVER_SCHEMADIR/02483_decimals.capnp:Message'"

rm $CLICKHOUSE_USER_FILES_UNIQUE/data.capnp
rm $CLICKHOUSE_USER_FILES_UNIQUE/02483_data.capnp

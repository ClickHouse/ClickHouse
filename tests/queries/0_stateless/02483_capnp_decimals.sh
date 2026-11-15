#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# It's important to copy the schema file before sourcing shell_config.sh,
# as the latter might initialize the ClickHouse client and server, which could
# cache the schema directory's contents.
mkdir -p /workspace/.clickhouse_runtime/format_schemas/test_02483
cp $CURDIR/format_schemas/02483_decimals.capnp /workspace/.clickhouse_runtime/format_schemas/test_02483/

# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

touch $CLICKHOUSE_USER_FILES/data.capnp

CLIENT_SCHEMADIR=$CURDIR/format_schemas
SERVER_SCHEMADIR=test_02483

$CLICKHOUSE_CLIENT -q "insert into function file('data.capnp', auto, 'decimal32 Decimal32(3), decimal64 Decimal64(6)') select 42.42, 4242.424242 settings format_schema='$SERVER_SCHEMADIR/02483_decimals.capnp:Message', engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file('data.capnp') settings format_schema='$SERVER_SCHEMADIR/02483_decimals.capnp:Message'"
$CLICKHOUSE_CLIENT -q "select * from file('data.capnp', auto, 'decimal64 Decimal64(6), decimal32 Decimal32(3)') settings format_schema='$SERVER_SCHEMADIR/02483_decimals.capnp:Message'"

rm $CLICKHOUSE_USER_FILES/data.capnp

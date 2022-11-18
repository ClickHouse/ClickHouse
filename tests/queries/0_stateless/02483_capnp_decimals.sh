#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_LOCAL -q "insert into function file(02483_data.capnp, auto, 'decimal32 Decimal32(3), decimal64 Decimal64(6)') select 42.42, 4242.424242 settings format_schema='format_schemas/02483_decimals.capnp:Message'"
$CLICKHOUSE_LOCAL -q "select * from file(02483_data.capnp) settings format_schema='format_schemas/02483_decimals.capnp:Message'"
$CLICKHOUSE_LOCAL -q "select * from file(02483_data.capnp, auto, 'decimal64 Decimal64(6), decimal32 Decimal32(3)') settings format_schema='format_schemas/02483_decimals.capnp:Message'"

rm 02483_data.capnp

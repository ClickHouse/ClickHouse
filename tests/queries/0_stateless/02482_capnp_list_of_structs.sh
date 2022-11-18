#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_LOCAL -q "insert into function file(02482_data.capnp, auto, 'nested Nested(x Int64, y Int64)') select [1,2], [3,4] settings format_schema='format_schemas/02482_list_of_structs.capnp:Nested'"
$CLICKHOUSE_LOCAL -q "select * from file(02482_data.capnp) settings format_schema='format_schemas/02482_list_of_structs.capnp:Nested'"
$CLICKHOUSE_LOCAL -q "select * from file(02482_data.capnp, auto, 'nested Nested(x Int64, y Int64)') settings format_schema='format_schemas/02482_list_of_structs.capnp:Nested'"
$CLICKHOUSE_LOCAL -q "select * from file(02482_data.capnp, auto, '\`nested.x\` Array(Int64), \`nested.y\` Array(Int64)') settings format_schema='format_schemas/02482_list_of_structs.capnp:Nested'"
$CLICKHOUSE_LOCAL -q "select * from file(02482_data.capnp, auto, '\`nested.x\` Array(Int64)') settings format_schema='format_schemas/02482_list_of_structs.capnp:Nested'"

rm 02482_data.capnp

#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "insert into table function file('uuid_str.msgpack', 'MsgPack', 'uuid UUID') select toUUID('5e7084e0-019f-461f-9e70-84e0019f561f') settings output_format_msgpack_uuid_representation='str'"
$CLICKHOUSE_CLIENT -q "select * from file('uuid_str.msgpack', 'MsgPack', 'uuid UUID')"

$CLICKHOUSE_CLIENT -q "insert into table function file('uuid_bin.msgpack', 'MsgPack', 'uuid UUID') select toUUID('5e7084e0-019f-461f-9e70-84e0019f561f') settings output_format_msgpack_uuid_representation='bin'"
$CLICKHOUSE_CLIENT -q "select * from file('uuid_bin.msgpack', 'MsgPack', 'uuid UUID')"

$CLICKHOUSE_CLIENT -q "insert into table function file('uuid_ext.msgpack', 'MsgPack', 'uuid UUID') select toUUID('5e7084e0-019f-461f-9e70-84e0019f561f') settings output_format_msgpack_uuid_representation='ext'"
$CLICKHOUSE_CLIENT -q "select * from file('uuid_ext.msgpack', 'MsgPack', 'uuid UUID')"
$CLICKHOUSE_CLIENT -q "select c1, toTypeName(c1) from file('uuid_ext.msgpack') settings input_format_msgpack_number_of_columns=1"


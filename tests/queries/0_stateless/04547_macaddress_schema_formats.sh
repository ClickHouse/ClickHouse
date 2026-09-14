#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CUR_DIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eo pipefail

# `MacAddress` in the formats that need an external schema. Protobuf accepts both a numeric
# field and a string field holding the textual MAC, as it does for `IPv4`.

BINARY_FILE_PATH=$(mktemp "$CLICKHOUSE_TMP/04547_macaddress.XXXXXX.binary")

echo "Protobuf, numeric field"
$CLICKHOUSE_CLIENT --allow_experimental_macaddress_type=1 --query \
    "SELECT toMacAddress('00:1a:2b:3c:4d:5e') AS mac FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/04547_macaddress:MacNumeric'" > "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --allow_experimental_macaddress_type=1 --query \
    "SELECT mac, toTypeName(mac) FROM file('$BINARY_FILE_PATH', 'Protobuf', 'mac MacAddress') SETTINGS format_schema='$SCHEMADIR/04547_macaddress:MacNumeric'"

echo "Protobuf, string field"
$CLICKHOUSE_CLIENT --allow_experimental_macaddress_type=1 --query \
    "SELECT toMacAddress('00:1a:2b:3c:4d:5e') AS mac FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/04547_macaddress:MacText'" > "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --allow_experimental_macaddress_type=1 --query \
    "SELECT mac, toTypeName(mac) FROM file('$BINARY_FILE_PATH', 'Protobuf', 'mac MacAddress') SETTINGS format_schema='$SCHEMADIR/04547_macaddress:MacText'"

echo "CapnProto"
$CLICKHOUSE_CLIENT --allow_experimental_macaddress_type=1 --query \
    "SELECT toMacAddress('00:1a:2b:3c:4d:5e') AS mac FORMAT CapnProto SETTINGS format_schema='$SCHEMADIR/04547_macaddress:MacNumeric'" > "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --allow_experimental_macaddress_type=1 --query \
    "SELECT mac, toTypeName(mac) FROM file('$BINARY_FILE_PATH', 'CapnProto', 'mac MacAddress') SETTINGS format_schema='$SCHEMADIR/04547_macaddress:MacNumeric'"

rm -f "$BINARY_FILE_PATH"

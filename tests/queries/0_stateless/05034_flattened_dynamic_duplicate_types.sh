#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two equal types in the list of types of a flattened Dynamic column map onto the same variant
# discriminator, so the data of the second one replaces the data of the first one while the offsets
# still refer to the first one, and reading the column goes out of bounds.
# To get such a list, write a valid one and rename a type into another one of the same name length.

$CLICKHOUSE_LOCAL -q "SELECT if(number = 0, 1::Int64::Dynamic, 1::UInt8::Dynamic) AS d FROM numbers(2) FORMAT Native
    SETTINGS output_format_native_use_flattened_dynamic_and_json_serialization = 1" \
    | sed 's/Int64/UInt8/g' > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.native"

$CLICKHOUSE_LOCAL -q "SELECT * FROM file('${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.native', Native)" 2>&1 \
    | grep -o "Duplicate type UInt8 in the list of types of a flattened Dynamic column"

rm "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.native"

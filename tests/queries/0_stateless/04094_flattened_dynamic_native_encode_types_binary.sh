#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test: flattened Dynamic serialization with encode_types_in_binary_format=1.
# encodeDataType at SerializationDynamic.cpp must use the two-arg overload
# that writes to the stream, not the one-arg overload that returns a String.

$CLICKHOUSE_LOCAL -m -q "
    CREATE TABLE test (d Dynamic(max_types=2)) ENGINE=Memory;
    INSERT INTO test VALUES (42::Int64), ('hello'), ('2020-01-01'::Date);
    SELECT * FROM test FORMAT Native SETTINGS
        output_format_native_use_flattened_dynamic_and_json_serialization=1,
        output_format_native_encode_types_in_binary_format=1;
" | $CLICKHOUSE_LOCAL --table test --input-format Native --input_format_native_decode_types_in_binary_format=1 -q "SELECT d, dynamicType(d) FROM test"

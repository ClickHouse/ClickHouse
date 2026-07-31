#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that reading a Native file with too many types in Dynamic
# serialization produces an informative error instead of an OOM.
#
# data_native/dynamic_flattened_too_many_types.native: flattened (version 3)
# serialization with num_types close to max size_t.
#
# data_native/dynamic_v2_too_many_types.native: V2 serialization with
# num_dynamic_types equal to SIZE_MAX, i.e. the exact value for which
# `num_dynamic_types + 1` wraps around to 0 if not checked beforehand.

# Flattened serialization path
$CLICKHOUSE_LOCAL --table test --input-format Native -q "SELECT * FROM test" < "${CUR_DIR}/data_native/dynamic_flattened_too_many_types.native" 2>&1 | grep -o 'Dynamic column has too many types: 9223372036854775807.*INCORRECT_DATA)'

# V2 serialization path
$CLICKHOUSE_LOCAL --table test --input-format Native -q "SELECT * FROM test" < "${CUR_DIR}/data_native/dynamic_v2_too_many_types.native" 2>&1 | grep -o 'Dynamic column has too many types: 18446744073709551615.*INCORRECT_DATA)'

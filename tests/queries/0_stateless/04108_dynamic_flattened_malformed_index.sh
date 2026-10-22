#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that reading a Native file with a corrupted index in flattened Dynamic
# serialization produces an informative error message instead of a crash.
#
# The file data_native/dynamic_flattened_bad_index.native is a pregenerated
# Native file with flattened Dynamic serialization for Dynamic(max_types=2)
# containing 3 rows: (42::Int64, 'hello', NULL). The NULL index byte (value 2)
# was changed to 10, which exceeds the valid range [0, 2].

$CLICKHOUSE_LOCAL --table test --input-format Native -q "SELECT * FROM test" < "${CUR_DIR}/data_native/dynamic_flattened_bad_index.native" 2>&1 | grep -o 'Incorrect index.*reserved for NULL values)'

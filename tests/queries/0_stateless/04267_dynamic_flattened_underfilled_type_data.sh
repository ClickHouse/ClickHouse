#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that reading a Native file with a flattened Dynamic column where the
# type data stream has fewer rows than declared by the indexes column produces
# an error instead of reading uninitialized memory.
#
# The file data_native/dynamic_flattened_underfilled.native is a truncated
# Native file with flattened Dynamic serialization for Dynamic(max_types=1)
# declaring 2 rows of FixedString(8) via the indexes column, but providing
# zero bytes of actual FixedString data.

$CLICKHOUSE_LOCAL --table test --input-format Native -q "SELECT * FROM test" < "${CUR_DIR}/data_native/dynamic_flattened_underfilled.native" 2>&1 | grep -o 'Mismatch in flattened Dynamic column[^.]*'

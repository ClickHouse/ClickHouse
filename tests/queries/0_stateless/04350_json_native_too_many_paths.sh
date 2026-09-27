#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Companion to 04307_dynamic_flattened_too_many_types for the JSON/Object family:
# reading a Native stream whose JSON/Object path count is corrupted (close to
# max size_t) must produce an informative INCORRECT_DATA error instead of an
# uncaught std::length_error or an OOM.
#
# data_native/object_flattened_too_many_paths.native: FLATTENED (version 3)
# serialization with the flattened-paths count equal to 2^63-1.
#
# data_native/object_v1_too_many_paths.native: non-flat (version 1) serialization
# with the dynamic-paths count equal to SIZE_MAX.

# FLATTENED serialization path (flattened_paths.resize)
$CLICKHOUSE_LOCAL --table test --input-format Native -q "SELECT * FROM test" < "${CUR_DIR}/data_native/object_flattened_too_many_paths.native" 2>&1 | grep -o 'JSON/Object column has too many paths: 9223372036854775807.*INCORRECT_DATA)'

# Non-flat serialization path (sorted_dynamic_paths->resize)
$CLICKHOUSE_LOCAL --table test --input-format Native -q "SELECT * FROM test" < "${CUR_DIR}/data_native/object_v1_too_many_paths.native" 2>&1 | grep -o 'JSON/Object column has too many paths: 18446744073709551615.*INCORRECT_DATA)'

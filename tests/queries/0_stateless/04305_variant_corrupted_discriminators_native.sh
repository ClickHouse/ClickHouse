#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that reading a Native file with an out-of-bounds Variant discriminator
# produces an informative INCORRECT_DATA error instead of a crash (heap OOB write).
#
# The file data_native/variant_corrupted_discriminators.native is a pregenerated
# Native file containing a single column `v` of type `Variant(String, UInt64)` with
# 3 rows. Valid discriminator values are 0 (String), 1 (UInt64), and 255 (NULL).
# The second row's discriminator was set to 2, which is out of bounds (>= num_variants).

$CLICKHOUSE_LOCAL --table test --input-format Native \
    -q "SELECT * FROM test" \
    < "${CUR_DIR}/data_native/variant_corrupted_discriminators.native" \
    2>&1 | grep -o 'Invalid discriminator value [0-9]* (num_variants = [0-9]*)'

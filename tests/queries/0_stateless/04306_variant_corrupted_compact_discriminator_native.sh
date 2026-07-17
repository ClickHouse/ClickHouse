#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that reading a Native file with an out-of-bounds discriminator in a
# COMPACT-mode Variant granule produces an INCORRECT_DATA error instead of a
# crash (heap OOB write).
#
# The file data_native/variant_corrupted_compact_discriminator.native contains
# a single column `v` of type `Variant(String, UInt64)` serialized in COMPACT
# mode with `compact_discr = 5`. Valid discriminator values are 0 (String),
# 1 (UInt64), and 255 (NULL); 5 is out of bounds (>= num_variants).

$CLICKHOUSE_LOCAL --table test --input-format Native \
    -q "SELECT * FROM test" \
    < "${CUR_DIR}/data_native/variant_corrupted_compact_discriminator.native" \
    2>&1 | grep -o 'Invalid discriminator value [0-9]* (num_variants = [0-9]*)'

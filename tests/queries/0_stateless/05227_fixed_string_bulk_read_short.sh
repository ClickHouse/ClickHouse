#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A FixedString column whose data ends in the middle of an element is reported where the read runs
# short, with the number of bytes it got, instead of being accepted as a shorter column.

# One declared row of FixedString(16) backed by 5 bytes.
$CLICKHOUSE_CLIENT --query "
SELECT * FROM format(Native, 'c FixedString(16)', unhex('010101630f4669786564537472696e67283136294141414141'));
" |& grep -o -m1 'Cannot read all data of type FixedString. Bytes read:5. String size:16.*CANNOT_READ_ALL_DATA'

# The element count can also come from the offsets of an array: two declared elements backed by 21 bytes.
$CLICKHOUSE_CLIENT --query "
SELECT * FROM format(Native, 'a Array(FixedString(16))', unhex('01010161164172726179284669786564537472696e6728313629290200000000000000414141414141414141414141414141414242424242'));
" |& grep -o -m1 'Cannot read all data of type FixedString. Bytes read:21. String size:16.*CANNOT_READ_ALL_DATA'

# A truncation on an element boundary is not this read's business: the array offsets catch it.
$CLICKHOUSE_CLIENT --query "
SELECT * FROM format(Native, 'a Array(FixedString(16))', unhex('01010161164172726179284669786564537472696e672831362929020000000000000041414141414141414141414141414141'));
" |& grep -o -m1 'Cannot read all array values: read just 1 of 2.*CANNOT_READ_ALL_DATA'

# A block whose data backs every declared element is read whole.
$CLICKHOUSE_CLIENT --query "
SELECT c FROM format(Native, 'c FixedString(4)', unhex('010201630e4669786564537472696e672834294141414142424242')) ORDER BY c;
"

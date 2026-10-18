#!/usr/bin/env bash
# Tags: long, no-fasttest, no-tsan, no-asan, no-msan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A part holding more than 1 GiB of Array(FixedString) elements in one read span must be readable.
# The read is split after 1 GiB / 1048576 = 1024 elements, so arr[1024] is the last element of the
# first part of the span and arr[1025] the only element of the second.
$CLICKHOUSE_CLIENT --query "
SET allow_suspicious_fixed_string_types = 1;
SET max_memory_usage = '8G';

DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt64, arr Array(FixedString(1048576))) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT 1, arrayMap(x -> toFixedString(toString(x), 1048576), range(1025));
SELECT arr[1] = toFixedString('0', 1048576),
       arr[1024] = toFixedString('1023', 1048576),
       arr[1025] = toFixedString('1024', 1048576) FROM t;
DROP TABLE t;
"

# An element count that the stream cannot back must still be refused instead of being preallocated.
# The Native block below declares 999999999 rows of FixedString(16), which is 14.9 GiB, and carries
# 32 bytes of payload. Reading it under an 8x smaller memory limit succeeds only if the destination
# is not grown to the declared size in one go.
# Block layout: 01 column, ff93ebdc03 rows, 0163 name "c", 0f + type name, then 32 payload bytes.
$CLICKHOUSE_CLIENT --query "
SET max_memory_usage = '4G';
SELECT count() FROM format(Native, 'c FixedString(16)', unhex('01ff93ebdc0301630f4669786564537472696e67283136294141414141414141414141414141414141414141414141414141414141414141'));
" 2>&1 | grep -o -m1 CANNOT_READ_ALL_DATA

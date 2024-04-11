#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query "insert into function file('${CLICKHOUSE_TMP}/test_support_hive_text_format.hivetext') select '20240305' as cstring, toInt32(100) as cint, toFloat32(1.22) as cfloat, tuple(123567, 'e01001', map('action1', 33333, 'act2', 5555)) as event, ['abc', 'def'] as locations, map('ddd', 2, 'eee', 6) as score, true as cbool, toInt8(1) as cbyte, toInt16(22) as cshort, toFloat64(333.333) as cdouble, toDate('2024-04-09') as d, timestamp('2022-01-02 00:00:00') as ts, X'41', 555.55, 66.66, [map('k1', 2, 'k2', 3), map('k3', 5, 'k5', 6)], [map('k1', tuple('1', map('k11', 'v22', 'k33', 'v55')), 'k2', tuple('11', map('k111', 'v222', 'k333', 'v555'))), map('k3', tuple('22', map('k22', 'v333', 'k33', 'v44')), 'k5', tuple('222', map('k222', 'v222')))];" 2>/dev/null

cat "${CLICKHOUSE_TMP}/test_support_hive_text_format.hivetext"

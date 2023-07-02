#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Code: 43. DB::Exception: Received from localhost:9000. DB::Exception: array aggregate functions cannot be performed on two empty arrays: While processing arrayJaccardIndex([], []). (ILLEGAL_TYPE_OF_ARGUMENT)
$CLICKHOUSE_CLIENT -q "SELECT arrayJaccardIndex([], [])" |& grep -o "Code: 43"

# Code: 386. DB::Exception: Received from localhost:9000. DB::Exception: There is no subtype for types UInt8, String because some of them are String/FixedString and some of them are not: While processing [1, 2] AS arr_1, ['1', '2'] AS arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2). (NO_COMMON_TYPE)
$CLICKHOUSE_CLIENT -q "select [1,2] as arr_1, ['1','2'] as arr_2, round(arrayJaccardIndex(arr_1, arr_2), 2)" |& grep -o "Code: 386"

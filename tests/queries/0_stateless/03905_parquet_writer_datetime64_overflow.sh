#!/usr/bin/env bash
# Tags: no-fasttest
# Parquet writer should not produce signed integer overflow when writing DateTime64 values
# near the boundary of representable range.
# https://github.com/ClickHouse/ClickHouse/issues/79261

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# DateTime64(7) is stored internally with 10^7 resolution, but Parquet uses nanoseconds (10^9),
# so the writer multiplies by 100. For dates near year 2263, this multiplication overflows Int64.
${CLICKHOUSE_CLIENT} -q "SELECT '2263-01-01 00:00:00'::DateTime64(7) FORMAT Parquet" 2>&1 | grep -o 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE'

# Verify that values within range still work fine.
${CLICKHOUSE_CLIENT} -q "SELECT '2200-01-01 00:00:00'::DateTime64(7) FORMAT Parquet" > /dev/null 2>&1 && echo "OK"

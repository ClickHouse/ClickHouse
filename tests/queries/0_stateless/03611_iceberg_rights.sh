#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir "${CLICKHOUSE_TMP}/foo"
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t0 (c0 Nullable(Int)) ENGINE = IcebergLocal('${CLICKHOUSE_TMP}/foo'); -- { serverError PATH_ACCESS_DENIED }
"

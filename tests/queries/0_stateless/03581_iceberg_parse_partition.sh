#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t0 (c0 Nullable(Int))
    ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES}/file0')
    PARTITION BY (c0.null IS NULL);  -- { serverError BAD_ARGUMENTS }
";

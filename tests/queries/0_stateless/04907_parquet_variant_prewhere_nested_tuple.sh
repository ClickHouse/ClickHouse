#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

SCHEMA='t Tuple(a Tuple(j JSON(k UInt64)))'
DATA_FILE="$CLICKHOUSE_TEST_UNIQUE_NAME.parquet"

: > "$CLICKHOUSE_USER_FILES/$DATA_FILE"

${CLICKHOUSE_CLIENT} --query "
    EXPLAIN PLAN
    SELECT t.a.j.k
    FROM file('$DATA_FILE', 'Parquet', '$SCHEMA')
    PREWHERE t.a.j.k = 1
" | grep -x '   Prewhere filter'

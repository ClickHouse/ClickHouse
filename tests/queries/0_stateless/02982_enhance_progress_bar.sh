#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -mn --query="DROP TABLE IF EXISTS 02982_enhance_progress_bar"

${CLICKHOUSE_CLIENT} -mn --query="
    CREATE TABLE 02982_enhance_progress_bar
    (
        id UInt64
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS index_granularity = 8192
"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&wait_end_of_query=1&query=INSERT+INTO+default.02982_enhance_progress_bar+(id)+SELECT+*+FROM+generateRandom()+LIMIT+1000"\
                      -v 2>&1 | grep 'X-ClickHouse-Summary' | sed 's/,\"elapsed_ns[^}]*//'


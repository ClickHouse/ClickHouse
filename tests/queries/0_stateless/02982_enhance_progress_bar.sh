#!/usr/bin/env bash
#Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -mn --query="DROP DATABASE IF EXISTS 02982_enhance_progress_bar"
${CLICKHOUSE_CLIENT} -mn --query="CREATE DATABASE IF NOT EXISTS 02982_enhance_progress_bar"
${CLICKHOUSE_CLIENT} -mn --query="
    CREATE TABLE 02982_enhance_progress_bar.test_bar
    (
        id UInt64
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS index_granularity = 8192
"
${CLICKHOUSE_CURL} "http://localhost:8123/?database=02982_enhance_progress_bar&wait_end_of_query=1&query=INSERT+INTO+test_bar+(id)+SELECT+*+FROM+generateRandom()+LIMIT+100000000"\
                      -v 2>&1 | grep 'X-ClickHouse-Summary' | sed 's/,\"elapsed_ns[^}]*//'

${CLICKHOUSE_CLIENT} -mn --query="DROP DATABASE IF EXISTS 02982_enhance_progress_bar"

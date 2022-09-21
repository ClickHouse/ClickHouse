#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --path "$CLICKHOUSE_TEST_UNIQUE_NAME" -nm -q "
    create table ttl_02335 (
        date Date,
        key Int,
        value String TTL date + interval 1 month
    )
    engine=MergeTree
    order by key
    settings
        min_bytes_for_wide_part=0,
        min_rows_for_wide_part=0;

    -- all_1_1_0
    -- all_1_1_1
    insert into ttl_02335 values ('2010-01-01', 2010, 'foo');
    -- all_1_1_2
    optimize table ttl_02335 final;
    -- all_1_1_3
    optimize table ttl_02335 final;
"

test -f "$CLICKHOUSE_TEST_UNIQUE_NAME"/data/_local/ttl_02335/all_1_1_3/value.bin && echo "[FAIL] value column should not exist"
exit 0

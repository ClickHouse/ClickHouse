#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_NAME="t_lc_single_dictionary_filtered_read"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS ${TABLE_NAME};
    CREATE TABLE ${TABLE_NAME}
    (
        id UInt64,
        s LowCardinality(String)
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS
        index_granularity = 64,
        index_granularity_bytes = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0;

    INSERT INTO ${TABLE_NAME}
    SELECT number, toString(number)
    FROM numbers(1024)
    SETTINGS low_cardinality_use_single_dictionary_for_part = 1;
"

trap '${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}"' EXIT

# Keep only a prefix of every granule so that `MergeTreeRangeReader::ReadResult::shrink`
# rebuilds the `LowCardinality` column through `cloneEmpty` and `insertRangeFrom`.
CLICKHOUSE_CLIENT_TRACE=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=trace/g")
${CLICKHOUSE_CLIENT_TRACE} --query_id="05045_${CLICKHOUSE_DATABASE}" -q "
    SELECT count()
    FROM
    (
        SELECT s, count()
        FROM ${TABLE_NAME}
        PREWHERE toUInt64(s) % 64 < 16
        GROUP BY s
    )
    SETTINGS
        max_threads = 1,
        max_block_size = 256,
        optimize_read_in_order = 0;
" 2>&1 >/dev/null | grep -F -c 'Aggregation method: low_cardinality_single_dictionary'

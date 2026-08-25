#!/usr/bin/env bash
# Tags: no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "drop table if exists tbl;"
${CLICKHOUSE_CLIENT} --query "create table tbl (timestamp DateTime, x UInt32 TTL timestamp + INTERVAL 1 MONTH, y UInt32 TTL timestamp + INTERVAL 1 DAY, index i x type minmax granularity 1, projection p (select x order by y)) engine MergeTree order by () settings min_bytes_for_wide_part = 1, index_granularity = 1;"
${CLICKHOUSE_CLIENT} --query "insert into tbl select today() - 100, 1, 2 union all select today() - 50, 2, 4;"

# Wait for column TTL to take effect
i=0
retries=300
while [[ $i -lt $retries ]]; do
    res=$($CLICKHOUSE_CLIENT -q "select x from tbl limit 1 format TSVRaw")
    if [[ $res -eq 0 ]]; then
        ${CLICKHOUSE_CLIENT} --query "select x, y from tbl;"
        ${CLICKHOUSE_CLIENT} --query "select x, y from tbl where x = 0;"
        ${CLICKHOUSE_CLIENT} --query "select x, y from tbl where y = 2 settings force_optimize_projection_name = 'p';"

        ${CLICKHOUSE_CLIENT} --query "drop table tbl;"
        exit 0
    fi
    ((++i))
    sleep 1
done

echo "Timeout waiting for column TTL to take effect" >&2
exit 1

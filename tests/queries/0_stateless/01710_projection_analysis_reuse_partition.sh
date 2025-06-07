#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "drop table if exists t"
${CLICKHOUSE_CLIENT} -q "create table t(s LowCardinality(String), e DateTime64(3), projection p1 (select * order by s, e)) engine MergeTree partition by toYYYYMM(e) order by tuple() settings index_granularity = 8192, index_granularity_bytes = '100M'"
${CLICKHOUSE_CLIENT} -q "insert into t select 'AAP', toDateTime('2023-07-01') + 360 * number from numbers(50000)"
${CLICKHOUSE_CLIENT} -q "insert into t select 'AAPL', toDateTime('2023-07-01') + 360 * number from numbers(50000)"

CLICKHOUSE_CLIENT_DEBUG_LOG=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=debug/g')

${CLICKHOUSE_CLIENT_DEBUG_LOG} -q "select count() from t where e >= '2023-11-08 00:00:00.000' and e < '2023-11-09 00:00:00.000' and s in ('AAPL') settings optimize_use_projection_filtering = 1 format Null" 2>&1 | grep -oh "Selected .* parts by partition key, *. parts by primary key, .* marks by primary key, .* marks to read from .* ranges.*$"

${CLICKHOUSE_CLIENT} -q "drop table t"

#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-random-detach
# no-random-detach: test checks system.parts
# add_minmax_index_for_numeric_columns=0: Would use the index and not the projection
# use_statistics_for_part_pruning=0: Statistics pruning would filter parts before projection check, causing force_optimize_projection_name to fail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "drop table if exists tbl;"
${CLICKHOUSE_CLIENT} --query "create table tbl (timestamp DateTime, x UInt32 TTL timestamp + INTERVAL 1 MONTH, y UInt32 TTL timestamp + INTERVAL 1 DAY, index i x type minmax granularity 1, projection p (select x order by y)) engine MergeTree order by () settings min_bytes_for_wide_part = 1, index_granularity = 1, add_minmax_index_for_numeric_columns=0;"

# Two separate inserts produce two parts; `OPTIMIZE FINAL` then merges them and applies the column TTL deterministically.
${CLICKHOUSE_CLIENT} --query "insert into tbl values (today() - 100, 1, 2);"
${CLICKHOUSE_CLIENT} --query "insert into tbl values (today() - 50, 2, 4);"

${CLICKHOUSE_CLIENT} --query "optimize table tbl final settings optimize_throw_if_noop = 1;"

${CLICKHOUSE_CLIENT} --query "select x, y from tbl;"
${CLICKHOUSE_CLIENT} --query "select x, y from tbl where x = 0;"
${CLICKHOUSE_CLIENT} --query "select x, y from tbl where y = 2 settings force_optimize_projection_name = 'p', use_statistics_for_part_pruning = 0;"

${CLICKHOUSE_CLIENT} --query "drop table tbl;"

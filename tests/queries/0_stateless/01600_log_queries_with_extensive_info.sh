#!/usr/bin/env bash
# Tags: no-parallel

set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "drop database if exists test_log_queries" "--query_id=01600_log_queries_with_extensive_info_000"
${CLICKHOUSE_CLIENT} -q "create database test_log_queries" "--query_id=01600_log_queries_with_extensive_info_001"
${CLICKHOUSE_CLIENT} -q "create table test_log_queries.logtable(i int, j int, k int) engine MergeTree order by i" "--query_id=01600_log_queries_with_extensive_info_002"
${CLICKHOUSE_CLIENT} -q "insert into test_log_queries.logtable values (1,2,3), (4,5,6)" "--query_id=01600_log_queries_with_extensive_info_003"
${CLICKHOUSE_CLIENT} -q "select k from test_log_queries.logtable where i = 4" "--query_id=01600_log_queries_with_extensive_info_004"

# exception query should also contain query_kind
${CLICKHOUSE_CLIENT} -q "select k from test_log_queries.logtable where i > ''" "--query_id=01600_log_queries_with_extensive_info_004_err" 2> /dev/null || true

${CLICKHOUSE_CLIENT} -q "select k from test_log_queries.logtable where i = 1" "--query_id=01600_log_queries_with_extensive_info_005"
${CLICKHOUSE_CLIENT} -q "select * from test_log_queries.logtable where i = 1" "--query_id=01600_log_queries_with_extensive_info_006"
${CLICKHOUSE_CLIENT} -q "create table test_log_queries.logtable2 as test_log_queries.logtable" "--query_id=01600_log_queries_with_extensive_info_007"
${CLICKHOUSE_CLIENT} -q "insert into test_log_queries.logtable2 select * from test_log_queries.logtable" "--query_id=01600_log_queries_with_extensive_info_008"
${CLICKHOUSE_CLIENT} -q "create table test_log_queries.logtable3 engine MergeTree order by j as select * from test_log_queries.logtable" "--query_id=01600_log_queries_with_extensive_info_009"
${CLICKHOUSE_CLIENT} -q "alter table test_log_queries.logtable rename column j to x, rename column k to y" "--query_id=01600_log_queries_with_extensive_info_010"
${CLICKHOUSE_CLIENT} -q "alter table test_log_queries.logtable2 add column x int, add column y int" "--query_id=01600_log_queries_with_extensive_info_011"
${CLICKHOUSE_CLIENT} -q "alter table test_log_queries.logtable3 drop column i, drop column k" "--query_id=01600_log_queries_with_extensive_info_012"
${CLICKHOUSE_CLIENT} -q "rename table test_log_queries.logtable2 to test_log_queries.logtable4, test_log_queries.logtable3 to test_log_queries.logtable5" "--query_id=01600_log_queries_with_extensive_info_013"
${CLICKHOUSE_CLIENT} -q "optimize table test_log_queries.logtable" "--query_id=01600_log_queries_with_extensive_info_014"
${CLICKHOUSE_CLIENT} -q "drop table if exists test_log_queries.logtable" "--query_id=01600_log_queries_with_extensive_info_015"
${CLICKHOUSE_CLIENT} -q "drop table if exists test_log_queries.logtable2" "--query_id=01600_log_queries_with_extensive_info_016"
${CLICKHOUSE_CLIENT} -q "drop table if exists test_log_queries.logtable3" "--query_id=01600_log_queries_with_extensive_info_017"
${CLICKHOUSE_CLIENT} -q "drop database if exists test_log_queries" "--query_id=01600_log_queries_with_extensive_info_018"

${CLICKHOUSE_CLIENT} -q "system flush logs"
${CLICKHOUSE_CLIENT} -q "select columns(query, normalized_query_hash, query_kind, databases, tables, columns) apply (any) from system.query_log where current_database = currentDatabase() AND type != 'QueryStart' and query_id like '01600_log_queries_with_extensive_info%' group by query_id order by query_id"

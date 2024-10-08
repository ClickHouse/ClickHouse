#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Avoid using threads in other parallel queries.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_OPTIONS=(
    "--log_query_threads=1"
    "--log_queries_min_type=QUERY_FINISH"
    "--log_queries=1"
    "--format=Null"
    "--use_concurrency_control=0"
)

UNIQUE_QUERY_ID="02871_1_$$"

# TCPHandler and QueryPullPipeEx threads are always part of the query thread group, but those threads are not within the max_threads limit.
${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_1" --query='SELECT 1' "${QUERY_OPTIONS[@]}"
${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_2" --query='SELECT 1 SETTINGS max_threads = 1' "${QUERY_OPTIONS[@]}"
${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_3" --query='SELECT 1 SETTINGS max_threads = 8' "${QUERY_OPTIONS[@]}"
${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_4" --query='SELECT * FROM numbers_mt(500000) SETTINGS max_threads = 1' "${QUERY_OPTIONS[@]}"
${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_5" --query='SELECT * FROM numbers_mt(500000) SETTINGS max_threads = 2' "${QUERY_OPTIONS[@]}"
${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_6" --query='SELECT * FROM numbers_mt(500000) SETTINGS max_threads = 4' "${QUERY_OPTIONS[@]}"
${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_7" --query='SELECT * FROM numbers_mt(5000), numbers(5000) SETTINGS max_threads = 1, joined_subquery_requires_alias=0' "${QUERY_OPTIONS[@]}"
${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_8" --query='SELECT * FROM numbers_mt(5000), numbers(5000) SETTINGS max_threads = 4, joined_subquery_requires_alias=0' "${QUERY_OPTIONS[@]}"

${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_9" -m --query="""
SELECT count() FROM 
    (SELECT number FROM numbers_mt(1,100000) 
            UNION ALL SELECT number FROM numbers_mt(10000, 200000)
            UNION ALL SELECT number FROM numbers_mt(30000, 40000)
            UNION ALL SELECT number FROM numbers_mt(30000, 40000)
            UNION ALL SELECT number FROM numbers_mt(300000, 400000)
            UNION ALL SELECT number FROM numbers_mt(300000, 400000)
            UNION ALL SELECT number FROM numbers_mt(300000, 4000000)
            UNION ALL SELECT number FROM numbers_mt(300000, 4000000)
    ) SETTINGS max_threads = 1""" "${QUERY_OPTIONS[@]}"

${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_10" -m --query="""
SELECT count() FROM 
    (SELECT number FROM numbers_mt(1,100000) 
            UNION ALL SELECT number FROM numbers_mt(10000, 2000)
            UNION ALL SELECT number FROM numbers_mt(30000, 40000)
            UNION ALL SELECT number FROM numbers_mt(30000, 40)
            UNION ALL SELECT number FROM numbers_mt(300000, 400)
            UNION ALL SELECT number FROM numbers_mt(300000, 4000)
            UNION ALL SELECT number FROM numbers_mt(300000, 40000)
            UNION ALL SELECT number FROM numbers_mt(300000, 4000000)
    ) SETTINGS max_threads = 4""" "${QUERY_OPTIONS[@]}"

${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_11" -m --query="""
SELECT count() FROM 
    (SELECT number FROM numbers_mt(1,100000) 
            UNION ALL SELECT number FROM numbers_mt(1, 1)
            UNION ALL SELECT number FROM numbers_mt(1, 1)
            UNION ALL SELECT number FROM numbers_mt(1, 1)
            UNION ALL SELECT number FROM numbers_mt(1, 1)
            UNION ALL SELECT number FROM numbers_mt(1, 1)
            UNION ALL SELECT number FROM numbers_mt(1, 1)
            UNION ALL SELECT number FROM numbers_mt(1, 4000000)
    ) SETTINGS max_threads = 4""" "${QUERY_OPTIONS[@]}"

${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_12" -m --query="""
SELECT sum(number) FROM numbers_mt(100000)
GROUP BY number % 2
WITH TOTALS ORDER BY number % 2
SETTINGS max_threads = 4""" "${QUERY_OPTIONS[@]}"

${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_13" -m --query="SELECT * FROM numbers(100000) SETTINGS max_threads = 1" "${QUERY_OPTIONS[@]}"

${CLICKHOUSE_CLIENT} --query_id="${UNIQUE_QUERY_ID}_14" -m --query="SELECT * FROM numbers(100000) SETTINGS max_threads = 4" "${QUERY_OPTIONS[@]}"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"
for i in {1..14}
do
    ${CLICKHOUSE_CLIENT} -m --query="""
    SELECT '${i}',
           peak_threads_usage, 
           (select count() from system.query_thread_log WHERE system.query_thread_log.query_id = '${UNIQUE_QUERY_ID}_${i}' AND current_database = currentDatabase()) = length(thread_ids),
           length(thread_ids) >= peak_threads_usage
    FROM system.query_log 
    WHERE type = 'QueryFinish' AND query_id = '${UNIQUE_QUERY_ID}_${i}' AND current_database = currentDatabase()"
done

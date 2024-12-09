#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: it checks the number of threads, which can be lowered in presence of other queries

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --log_queries=1 --max_threads=32 --use_concurrency_control=0 --query_id "2015_${CLICKHOUSE_DATABASE}_query" -q "select count() from remote('127.0.0.{2,3}', numbers(10)) where number global in (select number % 5 from numbers_mt(1000000))"
${CLICKHOUSE_CLIENT} -q "system flush logs"
${CLICKHOUSE_CLIENT} -q "select length(thread_ids) >= 32 from system.query_log where event_date >= yesterday() and query_id = '2015_${CLICKHOUSE_DATABASE}_query' and type = 'QueryFinish' and current_database = currentDatabase()"

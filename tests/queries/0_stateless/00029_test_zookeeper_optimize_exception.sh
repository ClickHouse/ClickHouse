#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_optimize_exception"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_optimize_exception_replicated"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_optimize_exception (date Date) ENGINE=MergeTree() PARTITION BY toYYYYMM(date) ORDER BY date"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_optimize_exception_replicated (date Date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test/optimize', 'r1') PARTITION BY toYYYYMM(date) ORDER BY date"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test_optimize_exception VALUES (toDate('2017-09-09')), (toDate('2017-09-10'))"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test_optimize_exception VALUES (toDate('2017-09-09')), (toDate('2017-09-10'))"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test_optimize_exception_replicated VALUES (toDate('2017-09-09')), (toDate('2017-09-10'))"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test_optimize_exception_replicated VALUES (toDate('2017-09-09')), (toDate('2017-09-10'))"

${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="OPTIMIZE TABLE test_optimize_exception PARTITION 201709 FINAL"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="OPTIMIZE TABLE test_optimize_exception_replicated PARTITION 201709 FINAL"

echo "$(${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --server_logs_file=/dev/null --query="OPTIMIZE TABLE test_optimize_exception PARTITION 201710" 2>&1)" \
  | grep -c 'Code: 388. DB::Exception: .* DB::Exception: .* Cannot select parts for optimization'
echo "$(${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --server_logs_file=/dev/null --query="OPTIMIZE TABLE test_optimize_exception_replicated PARTITION 201710" 2>&1)" \
  | grep -c 'Code: 388. DB::Exception: .* DB::Exception:.* Cannot select parts for optimization'

${CLICKHOUSE_CLIENT} --query="DROP TABLE test_optimize_exception NO DELAY"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test_optimize_exception_replicated NO DELAY"
sleep 1

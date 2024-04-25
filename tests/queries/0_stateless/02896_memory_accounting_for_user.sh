#!/usr/bin/env bash
# Tags: no-parallel, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


total_iterations=16
parallelism=32

$CLICKHOUSE_CLIENT --query='DROP TABLE IF EXISTS test_inserts'
$CLICKHOUSE_CLIENT --query='CREATE TABLE test_inserts ENGINE=Null AS system.numbers'

run_query() {
  ( $CLICKHOUSE_CLIENT --query='SELECT * FROM numbers_mt(1000000) FORMAT CSV' | $CLICKHOUSE_CLIENT --max_threads 8 --max_memory_usage_for_user 1073741824 -q 'INSERT INTO test_inserts FORMAT CSV' 2>/dev/null )
}

for ((i = 1; i <= total_iterations; i++)); do
  for ((j = 1; j <= parallelism; j++)); do
    run_query & pids+=($!)
  done

  EXIT_CODE=0
  new_pids=()
  for pid in "${pids[@]:0:parallelism}"; do
    CODE=0
    wait "${pid}" || CODE=$?
    run_query & new_pids+=($!)
    if [[ "${CODE}" != "0" ]]; then
        EXIT_CODE=1;
    fi
  done
  for pid in "${pids[@]:parallelism}"; do
    CODE=0
    wait "${pid}" || CODE=$?
    if [[ "${CODE}" != "0" ]]; then
        EXIT_CODE=1;
    fi
  done
  pids=("${new_pids[@]}")

  if [[ $EXIT_CODE -ne 0 ]]; then
    exit $EXIT_CODE
  fi
done

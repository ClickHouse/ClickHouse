#!/usr/bin/env bash
# Tags: no-fasttest, no-flaky-check, no-parallel, deadlock, long, replica
#  no-fasttest, no-flaky-check: test can run up to 10 minutes with sanitizers

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -euo pipefail

RUNS=${RUNS:-25}
THREADS_PER_JOB=${THREADS_PER_JOB:-4}
PRINT_LOGS=${PRINT_LOGS:-0}
QUERY_TIMEOUT=${QUERY_TIMEOUT:-30}

# clickhouse-client sometimes hangs forever along with the server despite of timeout settings, so we use the timeout util here.
CLICKHOUSE_CLIENT="timeout $QUERY_TIMEOUT $CLICKHOUSE_CLIENT"
CLICKHOUSE_DATABASE_TEST="${CLICKHOUSE_DATABASE}_03710_parallel_alter_comment_rename_selects"


log() {
  (( !PRINT_LOGS )) || echo "$@"
}

pids=()
cleanup() {
  local rc=$?
  log "Cleaning up with result code $rc..."

  log "Killing threads..."
  kill -9 "${pids[@]}" &>/dev/null || true
  wait "${pids[@]}" &>/dev/null || true

  log "Deleting databases..."
  local rc2=0
  $CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_TEST};
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_TEST}_2;
  " || rc2=$?
  if (( rc2 != 0 )); then
    echo "❌ Failed to delete databases: code $rc2"
    (( rc == 0 )) && rc=$rc2
  fi

  log "Exiting with code $rc"
  exit $rc
}
trap cleanup EXIT


test_query() {
  local rc=0 output
  start_ns=$(date +%s%N)
  output=$($CLICKHOUSE_CLIENT -q "$@" 2>&1) || rc=$?

  if (( rc == 0 || rc == 81 )); then
      sleep "$(printf "0.0%02d\n" $((RANDOM % 30 + 1)))" # 1-30ms
      return 0
  fi

  end_ns=$(date +%s%N)
  dur_ms=$(( (end_ns - start_ns) / 1000000 ))
  if (( rc == 124 )); then
    echo "❌ Query timed out(${dur_ms} ms): $*"
  else
    echo "❌ Query failed with code $rc(${dur_ms} ms): $* -> $output"
  fi
  exit $rc
}

run_rename_thread() {
  log "❕ Rename thread $1 started with PID $BASHPID"
  for i in $(seq 1 "$RUNS"); do
    test_query "RENAME DATABASE ${CLICKHOUSE_DATABASE_TEST}   TO ${CLICKHOUSE_DATABASE_TEST}_2 -- thread $1"
    test_query "RENAME DATABASE ${CLICKHOUSE_DATABASE_TEST}_2 TO ${CLICKHOUSE_DATABASE_TEST}   -- thread $1"
    (( i % 1000 == 0 )) && log "Rename thread $1: processed $i queries"
  done
  log "✅ Rename thread $1 finished"
}

run_alter_comment_thread() {
  log "❕ Alter comment thread $1 started with PID $BASHPID"
  for i in $(seq 1 "$RUNS"); do
    test_query "ALTER DATABASE ${CLICKHOUSE_DATABASE_TEST}   MODIFY COMMENT 'comment1_${1}_${i} -- thread $1'"
    test_query "ALTER DATABASE ${CLICKHOUSE_DATABASE_TEST}_2 MODIFY COMMENT 'comment2_${1}_${i}  -- thread $1'"
    (( i % 1000 == 0 )) && log "Alter comment thread $1: processed $i queries"
  done
  log "✅ Alter comment thread $1 finished"
}

run_selects_thread() {
  log "❕ Select thread $1 started with PID $BASHPID"
  for i in $(seq 1 "$RUNS"); do
    # to make tests_with_database_column in various_checks.sh happy: database = currentDatabase()
    test_query "SELECT * FROM system.databases -- thread $1"
    test_query "SELECT * FROM system.tables    -- thread $1"
    (( i % 1000 == 0 )) && log "Selects thread $1: $i runs done"
  done
  log "✅ Selects thread $1 finished"
}

run_for_engine() {
  log "⚙️ Creating database with Engine=$1"
  test_query "
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_TEST};
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_TEST}_2;
    CREATE DATABASE ${CLICKHOUSE_DATABASE_TEST} ENGINE=${1}${2};
  "
  log "⚙️ Created database ${CLICKHOUSE_DATABASE_TEST}"
  log "⚙️ Will execute $RUNS runs, $THREADS_PER_JOB threads per job."

  log "⚙️ Starting threads..."
  for i in $(seq 1 "$THREADS_PER_JOB"); do
    run_rename_thread "$i" & pids+=("$!")
    run_alter_comment_thread "$i" & pids+=("$!")
    run_selects_thread "$i" & pids+=("$!")
  done

  while true; do
    local rc=0
    wait -n || rc=$?
    (( rc == 127 )) && echo "OK for '$1'" && break
    (( rc != 0 )) && exit "$rc"
  done
}


run_for_engine "Atomic" ""
run_for_engine "Replicated" "('/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}_db', '{shard}', '{replica}')"

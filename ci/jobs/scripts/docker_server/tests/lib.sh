#!/usr/bin/env bash
# shellcheck disable=SC2034
# The file is sourced by other scripts; disable SC2034 (unused variable) warning

set -euo pipefail

currentDir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
GIT_ROOT_DIR=$(git -C "$currentDir" rev-parse --show-toplevel)
TESTS_LIB_DIR="${GIT_ROOT_DIR}/ci/tmp/docker-library/official-images/test"

CLICKHOUSE_TEST_SLEEP=3
CLICKHOUSE_TEST_TRIES=${CLICKHOUSE_TEST_TRIES:-5}
CLICKHOUSE_TEST_LOG_LINES=50

function cname {
  echo clickhouse-test-contained-$RANDOM-$RANDOM
}

# `docker logs` stops where the file logger takes over: the image logs to files, `<console>` off.
# `docker cp`, not `docker exec cat`: the distroless image has no shell, and an exited container
# cannot be `exec`ed. The official-images runner discards a failing test's stdout, keeping stderr.
function dumpServerLogs {
  local cid="$1" tmp log
  tmp="$(mktemp -d)" || return 0
  {
    echo "===== container ====="
    docker inspect -f 'status={{.State.Status}} exit_code={{.State.ExitCode}} oom_killed={{.State.OOMKilled}}' "$cid" || true
    echo "===== docker logs, last $CLICKHOUSE_TEST_LOG_LINES lines ====="
    docker logs --tail "$CLICKHOUSE_TEST_LOG_LINES" "$cid" 2>&1 || true
    for log in clickhouse-server.err.log clickhouse-server.log; do
      echo "===== $log, last $CLICKHOUSE_TEST_LOG_LINES lines ====="
      if docker cp "$cid:/var/log/clickhouse-server/$log" "$tmp/$log" > /dev/null 2>&1; then
        tail -n "$CLICKHOUSE_TEST_LOG_LINES" "$tmp/$log" || true
      else
        echo "(not available)"
      fi
    done
  } >&2
  rm -rf "$tmp" || true
  return 0
}

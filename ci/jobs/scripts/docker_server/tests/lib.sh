#!/usr/bin/env bash
# shellcheck disable=SC2034
# The file is sourced by other scripts; disable SC2034 (unused variable) warning

set -euo pipefail

currentDir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
GIT_ROOT_DIR=$(git -C "$currentDir" rev-parse --show-toplevel)
TESTS_LIB_DIR="${GIT_ROOT_DIR}/ci/tmp/docker-library/official-images/test"

CLICKHOUSE_TEST_SLEEP=3
CLICKHOUSE_TEST_TRIES=${CLICKHOUSE_TEST_TRIES:-10}
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

# The helpers below inspect a container from the host: the default image is built `FROM scratch` and has no
# `sed`, `test`, or `stat` to run inside it.

# The uid of the container's main process.
function serverUid {
  local cid="$1" pid
  pid="$(docker inspect -f '{{.State.Pid}}' "$cid")"
  docker top "$cid" -eo pid,uid | awk -v pid="$pid" '$1 == pid { print $2 }'
}

# The uid owning a directory in the container, or nothing if the path is not a directory. `docker cp` is done by
# the daemon, so it reads the directory regardless of its permissions and of the container's capabilities.
function directoryOwner {
  local cid="$1" path="$2"
  { docker cp "$cid:$path" - 2> /dev/null || true; } \
    | tar -tv --numeric-owner 2> /dev/null \
    | awk 'NR == 1 && substr($1, 1, 1) == "d" { split($2, ids, "/"); print ids[1] }'
}

function pathExists {
  docker cp "$1:$2" - > /dev/null 2>&1
}

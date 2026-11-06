#!/usr/bin/env bash
# shellcheck disable=SC2034
# The file is sourced by other scripts; disable SC2034 (unused variable) warning

set -euo pipefail

currentDir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
GIT_ROOT_DIR=$(git -C "$currentDir" rev-parse --show-toplevel)
TESTS_LIB_DIR="${GIT_ROOT_DIR}/ci/tmp/docker-library/official-images/test"

CLICKHOUSE_TEST_SLEEP=3
CLICKHOUSE_TEST_TRIES=5

function cname {
  echo clickhouse-test-contained-$RANDOM-$RANDOM
}

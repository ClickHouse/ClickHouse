#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./02661_read_from_archive.lib
. "$CUR_DIR"/02661_read_from_archive.lib

run_archive_test "tzst" "tar -caf"
#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `Lance` requires the Rust build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

. "${CUR_DIR}/data_lance/run_local_test.sh"
run_lance_local_test "04635_lance_local_count_record_batch"

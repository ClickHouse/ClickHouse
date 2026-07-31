#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `Lance` requires the Rust build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "${CUR_DIR}/../shell_config.sh"
. "${CUR_DIR}/data_lance/run_local_test.sh"
run_lance_local_test "04555_lance_local_virtual_only_scan_without_count_optimization"

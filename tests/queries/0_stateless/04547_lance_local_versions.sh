#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: `Lance` requires the Rust build.
# Tag no-msan: `Lance` is disabled in MSan builds, because the `ring` crate does not build with MSan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "${CUR_DIR}/../shell_config.sh"
. "${CUR_DIR}/data_lance/run_local_test.sh"
run_lance_local_test "04547_lance_local_versions"

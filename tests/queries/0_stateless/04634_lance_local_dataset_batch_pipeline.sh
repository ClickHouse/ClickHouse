#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag `no-fasttest`: `Lance` requires the Rust build.
# Tag no-msan: `Lance` is disabled in MSan builds, because the `ring` crate does not build with MSan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

. "${CUR_DIR}/data_lance/run_local_test.sh"
run_lance_local_test "04634_lance_local_dataset_batch_pipeline"

#!/usr/bin/env bash
# Tags: no-random-settings, no-parallel-replicas
# no-parallel-replicas: the row-policy `SELECT(a)` assertion is not stable under `ParallelReplicas`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=04510_select_access_rights_rewrite.lib
. "$CUR_DIR"/04510_select_access_rights_rewrite.lib

set -euo pipefail

trap cleanup_objects EXIT
cleanup_objects

test_select_where
test_select_prewhere
test_row_policy

#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the fast test build has no `DeltaLakeLocal`
# no-msan: the MSan build has no `DeltaLakeLocal` (`delta-kernel-rs` is not instrumented)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `DeltaLakeLocal` hides the secret data lake settings of its `SETTINGS` clause.
$CLICKHOUSE_FORMAT --oneline --query "CREATE TABLE test_delta_lake_local (key UInt64) ENGINE = DeltaLakeLocal('/tmp/delta') SETTINGS auth_header = 'plain_auth_header'"

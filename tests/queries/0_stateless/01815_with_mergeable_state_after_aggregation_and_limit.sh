#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# with_mergeable_state_after_aggregation will not stop after 1 row, while with_mergeable_state_after_aggregation_and_limit should
$CLICKHOUSE_CLIENT -q 'select * from system.numbers limit 1' --stage with_mergeable_state_after_aggregation_and_limit

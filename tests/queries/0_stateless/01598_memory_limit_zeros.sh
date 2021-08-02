#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --no_max_memory_usage_for_client --multiquery --testmode <<EOF
SET max_memory_usage = 1, max_untracked_memory = 1000000;
select 'test', count(*) from zeros_mt(1000000) where not ignore(zero); -- { serverError 241 }
EOF


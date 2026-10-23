#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A spilling hash join on one thread switches to grace hash join, which takes over the stored blocks.
# With logs at the `test` level the destructor of the in-memory join checks its byte counts,
# so they must not count the blocks it handed over.
${CLICKHOUSE_CLIENT} --send_logs_level=test --query "
    SELECT count() FROM (SELECT number AS k FROM numbers(1000000)) AS t1
    INNER JOIN (SELECT number AS k FROM numbers(1000000)) AS t2 USING (k)
    SETTINGS join_algorithm = 'hash', max_threads = 1, max_bytes_before_external_join = 4194304,
        max_bytes_ratio_before_external_join = 0, grace_hash_join_initial_buckets = 1, query_plan_join_swap_table = 0
" 2>/dev/null

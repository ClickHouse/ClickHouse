#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --allow_experimental_parallel_reading_from_replicas=1 --parallel_replicas_mode='read_tasks' -nm < "$CURDIR"/01099_parallel_distributed_insert_select.sql > /dev/null

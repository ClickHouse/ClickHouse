#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS memory_credits_part_log"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE memory_credits_part_log (n UInt64, s String) ENGINE = MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part = 0"

for i in 0 1; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO memory_credits_part_log SELECT number + $i * 10000, repeat('x', 4096) FROM numbers(10000)"
done

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE memory_credits_part_log FINAL"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS part_log"

${CLICKHOUSE_CLIENT} --query "SELECT ProfileEvents['MemoryCredits'] > 0 FROM system.part_log WHERE table = 'memory_credits_part_log' AND event_type = 'MergeParts' ORDER BY event_time_microseconds DESC LIMIT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE memory_credits_part_log"

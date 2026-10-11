#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-spill-disabled.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# Under a user memory limit of 64 MiB, the default `max_bytes_ratio_before_external_set` of 0.5 gives the set
# of `IN` a spill threshold of about 32 MiB, so `max_bytes_before_external_set = 0` alone does not disable
# spilling: both thresholds must be 0. The set holds 140,000 16-byte keys in 16 MiB, and spilling it takes
# about 19 MiB more, which does not fit under that threshold. The set decides before each chunk, so it spills
# with the first chunk of 1,024 rows after it grows to 16 MiB at 131,072 keys.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 --max_untracked_memory 0 \
    --max_block_size 1024 --max_memory_usage_for_user 67108864 --multiquery <<'SQL'
SELECT 'default ratio', count() FROM numbers(10) WHERE toUInt128(number) IN (SELECT toUInt128(number) FROM numbers(140000))
SETTINGS max_bytes_before_external_set = 0, log_comment = 'default ratio';
SELECT 'disabled', count() FROM numbers(10) WHERE toUInt128(number) IN (SELECT toUInt128(number) FROM numbers(140000))
SETTINGS max_bytes_before_external_set = 0, max_bytes_ratio_before_external_set = 0, log_comment = 'disabled';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsSpilledToDisk']
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;
SQL

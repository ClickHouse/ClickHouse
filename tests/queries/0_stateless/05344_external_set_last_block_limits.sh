#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-last-block-limits.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# A set on disk of 16700 hashes takes 33 blocks of up to 512 keys. Its directory takes 16 bytes per block,
# rounded up to the capacity of its array, which is 512 bytes for 32 blocks and 1024 bytes for 33. The merge
# delivers the keys in one batch. After that batch, the set has written 32 blocks and keeps the last one
# pending. So only the last block, which the set writes when it is complete, takes the directory past
# `max_bytes_in_set = 1000`. The query fails in the `throw` mode. In the `break` mode, it counts an overflow
# and finds every key, as a set in memory keeps the block that reaches its limits.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --ignore-error \
    --max_bytes_before_external_set 1 --max_threads 1 --max_block_size 1000000 2> "${LOCAL_DIR}/errors.log" <<'SQL'
SELECT 'throw', count() FROM numbers(16700) WHERE cityHash64(number) IN (SELECT cityHash64(number) FROM numbers(16700))
SETTINGS max_bytes_in_set = 1000, set_overflow_mode = 'throw', log_comment = 'throw';
SELECT 'break', count() FROM numbers(16700) WHERE cityHash64(number) IN (SELECT cityHash64(number) FROM numbers(16700))
SETTINGS max_bytes_in_set = 1000, set_overflow_mode = 'break', log_comment = 'break';

-- The report shows how each query ended, the sets that it spilled, and whether a set counted an overflow.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, if(type = 'QueryFinish', 'finished', errorCodeToName(exception_code)),
    ProfileEvents['SetsSpilledToDisk'], ProfileEvents['OverflowBreak'] > 0
FROM system.query_log
WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing') AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;
SQL

# The query over the limit fails with it.
grep -o -E 'Limit for IN-set exceeded, max bytes: [0-9.]+ B, current bytes: [0-9.]+ KiB' "${LOCAL_DIR}/errors.log"

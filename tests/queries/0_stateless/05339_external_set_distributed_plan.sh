#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-distributed-plan.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# With a threshold of 1 byte, a set that may spill does so before its first chunk. A distributed plan ships
# the values of its sets to its worker tasks, so they stay in memory. The distributed plan does not support
# `WITH TOTALS`, so the second query falls back to local execution and spills the set that it builds.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 \
    --max_bytes_before_external_set 1 --multiquery <<'SQL'
CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT number FROM numbers(100000);
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT 'distributed', count(), sum(k) FROM t WHERE k IN (SELECT number * 3 FROM numbers(10000))
SETTINGS distributed_plan_fallback_to_local_execution = 0, log_comment = 'distributed';
SELECT 'fallback', count(), sum(k) FROM t WHERE k IN (SELECT number * 3 FROM numbers(10000)) WITH TOTALS
SETTINGS log_comment = 'fallback';

-- The worker tasks of the distributed plan log their own queries with the same `log_comment`, and their
-- number depends on how the plan splits the read, so only the initial queries are reported.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsSpilledToDisk']
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;
SQL

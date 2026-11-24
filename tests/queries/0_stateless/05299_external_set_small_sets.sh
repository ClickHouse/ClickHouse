#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-small-sets.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# A set spills only once it takes 16 MiB or the threshold, whichever is smaller; a smaller set stays in memory
# whatever the rest of the query or spilling would need.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 --multiquery <<'SQL'
CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
INSERT INTO t SELECT number FROM numbers(1000000);

-- The aggregation of the subquery keeps query memory above the threshold, and it stays in memory whatever
-- memory the machine has free. The set of 100 keys stays in memory, so the primary key reads only one
-- granule, while the set of 1,000,000 keys spills.
SELECT 'aggregation, small set', count() FROM t
WHERE k IN (SELECT g % 100 FROM (SELECT number AS g FROM numbers(1000000) GROUP BY g))
SETTINGS max_bytes_before_external_set = '16M', max_bytes_ratio_before_external_group_by = 0,
    log_comment = 'aggregation, small set';
SELECT 'aggregation, large set', count() FROM t
WHERE k IN (SELECT g FROM (SELECT number AS g FROM numbers(1000000) GROUP BY g))
SETTINGS max_bytes_before_external_set = '16M', max_bytes_ratio_before_external_group_by = 0,
    log_comment = 'aggregation, large set';

-- Spilling needs more memory than a threshold of 1 MB leaves: a set of 10 keys stays in memory, and a set of
-- 1,000,000 keys spills once it takes the threshold.
SELECT 'threshold below the memory of spilling, small set', 5 IN (SELECT number FROM numbers(10))
SETTINGS max_bytes_before_external_set = '1M', log_comment = 'threshold below the memory of spilling, small set';
SELECT 'threshold below the memory of spilling, large set', count() FROM numbers(10)
WHERE number IN (SELECT number FROM numbers(1000000))
SETTINGS max_bytes_before_external_set = '1M', log_comment = 'threshold below the memory of spilling, large set';

-- The sets spilled by each query.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsSpilledToDisk']
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;

-- The aggregations exceed the threshold.
SELECT log_comment, memory_usage > 16000000
FROM system.query_log
WHERE type = 'QueryFinish' AND startsWith(log_comment, 'aggregation') AND current_database = currentDatabase()
ORDER BY event_time_microseconds;

-- The set in memory lets the primary key select one mark.
SELECT 'marks', ProfileEvents['SelectedMarks']
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment = 'aggregation, small set' AND current_database = currentDatabase();
SQL

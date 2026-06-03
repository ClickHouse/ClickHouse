#!/usr/bin/env bash

# Regression test for https://github.com/ClickHouse/ClickHouse/pull/106021#discussion_r3349988224
#
# A part can start with an unfinished expired GROUP BY TTL and an unfinished
# rows DELETE TTL whose boundary is still in the future. The first TTL merge is
# legitimate: it applies the GROUP BY rule. After that merge, the part has a
# finished expired GROUP BY entry next to the future unfinished DELETE entry.
# TTLRowDeleteMergeSelector must use the smallest unfinished rows-affecting TTL
# when choosing centers, otherwise the finished GROUP BY watermark can keep
# causing spurious TTLDelete merges.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE=t_ttl_future_delete_groupby_no_reloop

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE $TABLE
(
    \`key\` UInt32,
    \`ts_group\` DateTime,
    \`ts_delete\` DateTime,
    \`value\` UInt32
)
ENGINE = MergeTree
ORDER BY (key)
TTL
    ts_delete + INTERVAL 1 SECOND DELETE,
    ts_group + INTERVAL 1 SECOND GROUP BY key SET value = sum(value)
SETTINGS
    min_bytes_for_wide_part = 0,
    merge_with_ttl_timeout = 0;
"

$CLICKHOUSE_CLIENT --query "
INSERT INTO $TABLE
SELECT
    number AS key,
    toDateTime('2000-01-01 00:00:00') AS ts_group,
    toDateTime('2100-01-01 00:00:00') AS ts_delete,
    1 AS value
FROM numbers(1, 500);
"

# Drive the first, valid TTL merge that applies the unfinished GROUP BY rule.
# Non-FINAL OPTIMIZE reaches the TTL selector; the background scheduler may
# also get there first.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE" 2>/dev/null || true

for _ in $(seq 1 60); do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log"

    TTL_MERGES=$($CLICKHOUSE_CLIENT --query "
        SELECT count()
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = '$TABLE'
          AND event_type = 'MergeParts'
          AND merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge')
    ")

    ACTIVE_MERGES=$($CLICKHOUSE_CLIENT --query "
        SELECT count()
        FROM system.merges
        WHERE database = currentDatabase()
          AND table = '$TABLE'
    ")

    if [ "$TTL_MERGES" != "0" ] && [ "$ACTIVE_MERGES" = "0" ]; then
        break
    fi

    sleep 0.25
done

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log"

SNAPSHOT=$($CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.part_log
    WHERE database = currentDatabase()
      AND table = '$TABLE'
      AND event_type = 'MergeParts'
      AND merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge')
")

# Give the background scheduler time to fire any spurious re-merge on the part
# whose GROUP BY TTL is finished and whose DELETE TTL is still in the future.
sleep 3
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS part_log"

FINAL=$($CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.part_log
    WHERE database = currentDatabase()
      AND table = '$TABLE'
      AND event_type = 'MergeParts'
      AND merge_reason IN ('TTLDropMerge', 'TTLDeleteMerge')
")

echo $((FINAL - SNAPSHOT))

$CLICKHOUSE_CLIENT --query "DROP TABLE $TABLE"

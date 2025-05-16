#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q 'DROP TABLE IF EXISTS t_part_log_has_merge_type_table'

${CLICKHOUSE_CLIENT} -q '
    CREATE TABLE t_part_log_has_merge_type_table
    (
        event_time DateTime,
        UserID UInt64,
        Comment String
    )
    ENGINE = MergeTree()
    ORDER BY tuple()
    TTL event_time + INTERVAL 3 MONTH
    SETTINGS old_parts_lifetime = 1, min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = true, max_number_of_merges_with_ttl_in_pool = 100
'

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_part_log_has_merge_type_table VALUES (now(), 1, 'username1');"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_part_log_has_merge_type_table VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');"

function get_parts_count() {
    table_name=$1
    ${CLICKHOUSE_CLIENT} -q '
        SELECT
            count(*)
        FROM
            system.parts
        WHERE
            table = '"'${table_name}'"'
        AND
            active = 1
        AND
            database = '"'${CLICKHOUSE_DATABASE}'"'
    '
}

function wait_table_parts_are_merged_into_one_part() {
    table_name=$1

    while true
    do
        count=$(get_parts_count $table_name)
        if [[ count -gt 1 ]]
        then
            sleep 1
        else
            break
        fi
    done
}

export -f get_parts_count
export -f wait_table_parts_are_merged_into_one_part

timeout 60 bash -c 'wait_table_parts_are_merged_into_one_part t_part_log_has_merge_type_table'

${CLICKHOUSE_CLIENT} -q 'SYSTEM FLUSH LOGS part_log'

${CLICKHOUSE_CLIENT} -q '
  SELECT
      event_type,
      merge_reason
  FROM
      system.part_log
  WHERE
          event_type = '"'MergeParts'"'
      AND
          table = '"'t_part_log_has_merge_type_table'"'
      AND
          database = '"'${CLICKHOUSE_DATABASE}'"'
  ORDER BY event_type, merge_reason
'

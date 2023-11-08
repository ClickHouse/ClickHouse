#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -nm -q "
DROP TABLE IF EXISTS enabled_part_check_t SYNC;
DROP TABLE IF EXISTS no_part_check_t SYNC;

CREATE TABLE enabled_part_check_t(k UInt32, v String) ENGINE ReplicatedMergeTree('/{database}/part_check', 'r1') ORDER BY k SETTINGS background_part_check_time_to_total_time_ratio=0.01, background_part_check_delay_seconds=0;

insert into enabled_part_check_t select number, toString(number) from numbers(1000);

CREATE TABLE no_part_check_t(k UInt32, v String) ENGINE ReplicatedMergeTree('/{database}/no_part_check', 'r1') ORDER BY k SETTINGS background_part_check_time_to_total_time_ratio=0, background_part_check_delay_seconds=0;

insert into no_part_check_t select number, toString(number) from numbers(1000);
"

for _ in {0..10}; do
  count=$($CLICKHOUSE_CLIENT -nm -q "
    SYSTEM FLUSH LOGS;

    SELECT count() > 0
    FROM system.text_log
    WHERE logger_name ILIKE '%' || currentDatabase() || '%enabled_part_check_t%ReplicatedMergeTreePartCheckThread%' AND message ILIKE '%Background part check succeeded%' AND event_date >= yesterday()
    GROUP BY logger_name;
  ")
  if [[ $count -eq 1 ]]; then
    echo 1;
    break;
  fi

  sleep 1

done

${CLICKHOUSE_CLIENT} -nm -q "
SELECT count()
FROM system.text_log
WHERE logger_name ILIKE '%' || currentDatabase() || '%no_part_check_t%ReplicatedMergeTreePartCheckThread%' AND message ILIKE '%Background part check%' AND event_date >= yesterday();"

${CLICKHOUSE_CLIENT} -nm -q "
DROP TABLE enabled_part_check_t SYNC;
DROP TABLE no_part_check_t SYNC;
"

#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

${CLICKHOUSE_CLIENT} -q 'DROP TABLE IF EXISTS table_with_single_pk'

${CLICKHOUSE_CLIENT} -q '
    CREATE TABLE table_with_single_pk
    (
      key UInt8,
      value String
    )
    ENGINE = MergeTree
    ORDER BY key
    SETTINGS old_parts_lifetime=0
'

${CLICKHOUSE_CLIENT} -q 'INSERT INTO table_with_single_pk SELECT number, toString(number % 10) FROM numbers(1000000)'

# Check NewPart
${CLICKHOUSE_CLIENT} -q 'SYSTEM FLUSH LOGS'
${CLICKHOUSE_CLIENT} -q "
    WITH (
         SELECT (event_time, event_time_microseconds)
         FROM system.part_log
         WHERE table = 'table_with_single_pk' AND database = currentDatabase() AND event_type = 'NewPart'
         ORDER BY event_time DESC
         LIMIT 1
    ) AS time
  SELECT if(dateDiff('second', toDateTime(time.2), toDateTime(time.1)) = 0, 'ok', 'fail')"

# Now let's check RemovePart
${CLICKHOUSE_CLIENT} -q 'TRUNCATE TABLE table_with_single_pk'
wait_for_delete_inactive_parts table_with_single_pk

${CLICKHOUSE_CLIENT} -q 'SYSTEM FLUSH LOGS;'
${CLICKHOUSE_CLIENT} -q "
    WITH (
         SELECT (event_time, event_time_microseconds)
         FROM system.part_log
         WHERE table = 'table_with_single_pk' AND database = currentDatabase() AND event_type = 'RemovePart'
         ORDER BY event_time DESC
         LIMIT 1
    ) AS time
    SELECT if(dateDiff('second', toDateTime(time.2), toDateTime(time.1)) = 0, 'ok', 'fail')"

${CLICKHOUSE_CLIENT} -q 'DROP TABLE table_with_single_pk'



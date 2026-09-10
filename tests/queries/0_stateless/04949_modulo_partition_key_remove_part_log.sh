#!/usr/bin/env bash
# Tags: no-shared-merge-tree
# no-shared-merge-tree: the wait below drives cleanup with SYSTEM START CLEANUP, which
# StorageSharedMergeTree does not implement, so the bounded wait would have no bound there.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `RemovePart` rows come from a third renderer, `MergeTreeData::removePartsFinally`, so the
# divergent partition key has to be observed there too.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE mod_remove (c0 Int128) ENGINE = MergeTree ORDER BY tuple()
PARTITION BY (CAST(37528, 'UInt64') % c0) SETTINGS old_parts_lifetime = 0"

$CLICKHOUSE_CLIENT -q "INSERT INTO mod_remove VALUES (167682982)"
$CLICKHOUSE_CLIENT -q "ALTER TABLE mod_remove DROP PARTITION 37528"

# Part removal is asynchronous, so the row can land after DROP PARTITION returns. START CLEANUP
# schedules a pass on every iteration.
TIMEOUT=60
TIMELIMIT=$((SECONDS+TIMEOUT))
while [ $SECONDS -lt "$TIMELIMIT" ]
do
    $CLICKHOUSE_CLIENT -q "SYSTEM START CLEANUP mod_remove"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log"
    logged=$($CLICKHOUSE_CLIENT -q "
        SELECT count() > 0 FROM system.part_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND database = currentDatabase() AND table = 'mod_remove'
            AND event_type = 'RemovePart'")
    if [ "$logged" = 1 ]
    then
        break
    fi
    sleep 1
done

$CLICKHOUSE_CLIENT -q "
SELECT 'remove part', partition FROM system.part_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND database = currentDatabase() AND table = 'mod_remove' AND event_type = 'RemovePart'
GROUP BY partition"

$CLICKHOUSE_CLIENT -q "DROP TABLE mod_remove"

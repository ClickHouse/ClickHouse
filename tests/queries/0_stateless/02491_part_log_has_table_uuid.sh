#!/usr/bin/env bash
# Tags: no-ordinary-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create table data_02491 (key Int) engine=MergeTree() order by tuple() settings old_parts_lifetime=600"
$CLICKHOUSE_CLIENT -q "insert into data_02491 values (1)"
$CLICKHOUSE_CLIENT -q "optimize table data_02491 final"
$CLICKHOUSE_CLIENT -q "truncate table data_02491"

# Part removal is asynchronous: whichever cleanup pass grabs the part writes the RemovePart row,
# so it can land after TRUNCATE returns. START CLEANUP schedules a pass on every iteration, since
# a cleanup thread that found nothing to do sleeps up to max_cleanup_delay_period.
TIMEOUT=60
TIMELIMIT=$((SECONDS+TIMEOUT))
while [ $SECONDS -lt "$TIMELIMIT" ]
do
    $CLICKHOUSE_CLIENT -q "system start cleanup data_02491"
    $CLICKHOUSE_CLIENT -q "system flush logs part_log"
    logged=$($CLICKHOUSE_CLIENT -q "
        select count() > 0
        from system.part_log
        where event_date >= yesterday() AND event_time >= now() - 600 and
            database = currentDatabase() and
            table = 'data_02491' and
            event_type = 'RemovePart' and
            part_name = 'all_1_1_1'")
    if [ "$logged" = 1 ]
    then
        break
    fi
    sleep 1
done

$CLICKHOUSE_CLIENT -q "
with (select uuid from system.tables where database = currentDatabase() and table = 'data_02491') as table_uuid_
select
    table_uuid != toUUIDOrDefault(Null),
    event_type,
    merge_reason,
    part_name
from system.part_log
where event_date >= yesterday() AND event_time >= now() - 600 AND
    database = currentDatabase() and
    table = 'data_02491' and
    table_uuid = table_uuid_
order by event_time_microseconds"

$CLICKHOUSE_CLIENT -q "drop table data_02491"

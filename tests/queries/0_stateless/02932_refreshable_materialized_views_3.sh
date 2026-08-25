#!/usr/bin/env bash
# Tags: atomic-database, memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Coverage for REFRESH dependency variants enabled by the dependencies overhaul:
#   - REFRESH AFTER 0 SECOND parsing
#   - REFRESH AFTER N SECOND DEPENDS ON ... (previously rejected at parse time)
#   - REFRESH DEPENDS ON ... shorthand (= REFRESH AFTER 0 SECOND DEPENDS ON ...)
#   - bare REFRESH is rejected
#   - MissingDependencies state and recovery when a dependency appears
#   - Mixed REFRESH EVERY <-> REFRESH AFTER dependencies
#   - Circular dependencies (3-view batched-stream-processing pattern)

CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g'`"
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --session_timezone Etc/UTC"`"

# system.view_refreshes briefly reports the transient internal state 'Scheduling' between
# state transitions of the refresh task. Use this helper for SELECTs that read the status
# column: it retries until no row reports 'Scheduling'.
query_no_scheduling() {
    local out
    while :
    do
        out=$($CLICKHOUSE_CLIENT -q "$1")
        if ! grep -qE $'(^|\t)Scheduling(\t|$)' <<< "$out"; then
            echo "$out"
            return
        fi
        sleep 0.2
    done
}

$CLICKHOUSE_CLIENT -q "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"


# --- Parser: REFRESH AFTER 0 SECOND, REFRESH AFTER N DEPENDS ON, REFRESH DEPENDS ON, bare REFRESH ---

# Use SYSTEM STOP VIEW immediately after each create to keep the views from spinning while we run other commands.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int8) engine Memory as select 1;

    -- REFRESH AFTER 0 SECOND parses (zero interval is now allowed for AFTER).
    create materialized view zero_after refresh after 0 second (x Int8) engine Memory empty as select x from src;
    system stop view zero_after;
    show create zero_after;

    -- REFRESH AFTER N DEPENDS ON ... (previously a BAD_ARGUMENTS parser error).
    create materialized view after_with_deps refresh after 1 hour depends on zero_after (x Int8) engine Memory empty as select x from src;
    system stop view after_with_deps;
    show create after_with_deps;

    -- REFRESH DEPENDS ON ... shorthand. Parser rewrites it to REFRESH AFTER 0 SECOND DEPENDS ON ...
    create materialized view shorthand_deps refresh depends on zero_after (x Int8) engine Memory empty as select x from src;
    system stop view shorthand_deps;
    show create shorthand_deps;

    -- Bare REFRESH (no EVERY/AFTER and no DEPENDS ON) is rejected at parse time.
    create materialized view bare refresh (x Int8) engine Memory empty as select x from src; -- { clientError SYNTAX_ERROR }

    drop table after_with_deps;
    drop table shorthand_deps;
    drop table zero_after;
    drop table src;"


# --- MissingDependencies state and recovery ---

# Depend on a regular (non-refreshable) MergeTree table. View must enter MissingDependencies and never refresh.
$CLICKHOUSE_CLIENT -q "
    create table plain (x Int64) engine MergeTree order by x;
    create materialized view miss refresh every 1 year depends on plain (x Int64) engine Memory as select x from plain;"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'miss' -- $LINENO" | xargs`" != 'MissingDependencies' ]
do
    sleep 0.5
done
query_no_scheduling "select '<1: missing>', status, last_success_time is null, next_refresh_time is null from refreshes where view = 'miss'"
$CLICKHOUSE_CLIENT -q "
    -- Modify dependency to a typo / non-existent name. Still MissingDependencies.
    alter table miss modify refresh every 1 year depends on nonexistent;"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'miss' -- $LINENO" | xargs`" != 'MissingDependencies' ]
do
    sleep 0.5
done
query_no_scheduling "select '<2: still missing>', status from refreshes where view = 'miss'"

# Now create a refreshable view named 'nonexistent'. 'miss' should pick up the dependency.
$CLICKHOUSE_CLIENT -q "
    create table src2 (x Int64) engine Memory as select 7;
    create materialized view nonexistent refresh every 1 year (x Int64) engine Memory as select x from src2;
    system wait view nonexistent;"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'miss' -- $LINENO" | xargs`" != 'Scheduled' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<3: recovered>', status not in ('MissingDependencies'), last_success_time is not null from refreshes where view = 'miss';"

# Drop the dependency. 'miss' must transition back to MissingDependencies.
$CLICKHOUSE_CLIENT -q "drop table nonexistent;"
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'miss' -- $LINENO" | xargs`" != 'MissingDependencies' ]
do
    sleep 0.5
done
query_no_scheduling "select '<4: dep dropped>', status, next_refresh_time is null from refreshes where view = 'miss'"
$CLICKHOUSE_CLIENT -q "
    drop table miss;
    drop table src2;
    drop table plain;"


# --- Mixed REFRESH EVERY / REFRESH AFTER dependencies ---

# (i) REFRESH AFTER 0 SECOND depends on REFRESH EVERY (shorthand DEPENDS ON): dependent refreshes immediately after dep refreshes.
$CLICKHOUSE_CLIENT -q "
    create table src3 (x Int64) engine Memory as select 100;
    create materialized view every_dep refresh every 1 year (x Int64) engine Memory as select x from src3;
    system wait view every_dep;
    create materialized view after_dep refresh depends on every_dep (x Int64) engine Memory as select x*2 as x from every_dep;"
while [ "`$CLICKHOUSE_CLIENT -q "select last_success_time is null from refreshes where view = 'after_dep' -- $LINENO" | xargs`" != '0' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "select '<5: after-on-every>', * from after_dep;"

# Force the dependency to refresh again, dependent should pick up the new value.
$CLICKHOUSE_CLIENT -q "
    truncate src3;
    insert into src3 values (200);
    system test view every_dep set fake time '2070-01-01 00:00:00';"
while [ "`$CLICKHOUSE_CLIENT -q "select min(x) from after_dep -- $LINENO" | xargs`" != '400' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<6: after-on-every refreshed>', * from after_dep;
    drop table after_dep;
    drop table every_dep;
    drop table src3;"

# (ii) REFRESH EVERY depends on REFRESH AFTER (mixed kinds; the dep has no next_refresh_timeslot, so we go through the all_advanced branch).
$CLICKHOUSE_CLIENT -q "
    create table src4 (x Int64) engine Memory as select 11;
    create materialized view after_src refresh after 1 year (x Int64) engine Memory as select x from src4;
    system wait view after_src;
    create materialized view every_on_after refresh every 1 year depends on after_src (x Int64) engine Memory as select x+1 as x from after_src;"
# every_on_after's first refresh: REFRESH EVERY, mixed kinds → all_kind_every is false → all_advanced branch.
# after_src.last_success_end_time > 0 (just refreshed), so all_advanced = true.
# REFRESH EVERY uses timeslot from schedule, when = timeslot < now → refresh immediately.
while [ "`$CLICKHOUSE_CLIENT -q "select last_success_time is null from refreshes where view = 'every_on_after' -- $LINENO" | xargs`" != '0' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '<7: every-on-after>', * from every_on_after;
    drop table every_on_after;
    drop table after_src;
    drop table src4;"


# --- Circular dependencies (3-view batched stream processing) ---

# Setup mirrors the docs example. No server restart in stateless test, so the cycle keeps spinning after one manual kick.
$CLICKHOUSE_CLIENT -q "
    create table cur_batch (t UInt64, v Int64) engine MergeTree order by t;
    create table batch_log (max_t UInt64, n Int64) engine MergeTree order by max_t;
    create table stats (h UInt64, n UInt64) engine SummingMergeTree order by h;

    create materialized view current_batch_v refresh after 1 second depends on batch_log_v, stats_v to cur_batch as
        select number as t, number * 10 as v
        from system.numbers
        where number > (select max(max_t) from batch_log)
        limit 5;

    create materialized view batch_log_v refresh depends on current_batch_v append to batch_log as
        select max(t) as max_t, count() as n from cur_batch;

    create materialized view stats_v refresh depends on current_batch_v append to stats as
        select cityHash64(v) % 4 as h, count() as n from cur_batch group by h;

    -- Initial state has no last-known dep refreshes to compare against, so the cycle won't start by itself; kick it.
    system refresh view current_batch_v;"

# Wait until at least 3 waves have accumulated in batch_log (one append per wave).
for _ in $(seq 1 120); do
    n=$($CLICKHOUSE_CLIENT -q "select count() from batch_log")
    if [ "$n" -ge 3 ]; then break; fi
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    -- Strictly increasing max_t shows reader saw fresh dep state every wave; n>0 shows non-empty batches.
    select '<8: circular waves>', count() >= 3, count(distinct max_t) = count(), min(n) > 0 from batch_log;
    -- stats accumulated rows on the same wave cadence.
    select '<9: stats nonempty>', sum(n) > 0 from stats;
    drop table current_batch_v;
    drop table batch_log_v;
    drop table stats_v;
    drop table cur_batch;
    drop table batch_log;
    drop table stats;
    drop table refreshes;"

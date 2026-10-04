#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-ordinary-database
# no-parallel: the failpoints below are server-wide.
#
# A mutate task stamps `removal_tid` on its source part and then looks up whether that stamp's
# transaction committed. If the transaction is rolled back during the lookup, the task reads
# `RolledBackCSN` and blanks the stamp. The rollback's own `resetRemovalTID` then finds nothing to
# clear, so it treats the part as owned by another operation and never restores it to Active.
#
# The part is left Outdated with no removal stamp: still readable by every transaction, because
# visibility comes from the version metadata, but no longer retirable by a merge, which only takes
# parts out of the Active set. A following `OPTIMIZE FINAL` copies its row into the merged part and
# cannot retire the original, so the row is returned twice, and a transactional ALTER blocked by
# that part finds no removal lock, reports no write conflict, and waits to `max_execution_time`.
#
# Which of the sources gets poisoned depends on which mutate task is mid-lookup when the rollback
# is marked, and on a cold server the rollback sometimes wins outright, so the attempt is repeated.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

FP_SLOW=transaction_slow_resolve_removal_csn
FP_ROLLBACK=transaction_rollback_pause_after_mark

cleanup() {
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_SLOW" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_ROLLBACK" 2>/dev/null
}
trap cleanup EXIT

# A transactional ALTER stops waiting only when a blocking part reports a write conflict, and that
# conflict comes from the part's removal lock. `max_execution_time` is kept short so a regression is
# a wrong answer rather than a slow test.
tx_short() {
    local session="${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}_tx$1"
    ${CLICKHOUSE_CURL} --max-time 30 -sSk \
        "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?session_id=$session&database=$CLICKHOUSE_DATABASE&max_execution_time=10" \
        --data "$2"
}

# Leaves the four sources of `mt` rolled back, with one of them at risk of losing its stamp.
poison() {
    local tx=$1
    $CLICKHOUSE_CLIENT -q "drop table if exists mt"
    $CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"
    for v in 1 2 3 4; do
        $CLICKHOUSE_CLIENT -q "insert into mt values ($v)"
    done

    tx "$tx" "begin transaction" >/dev/null
    local tid
    tid=$(tx "$tx" "select transactionID()" | grep -Po "\(.*")

    # Each mutate task now lingers in the CSN lookup of the stamp it just wrote.
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP_SLOW"
    tx_async "$tx" "alter table mt update n = n * 10 where 1" >/dev/null
    sleep 0.4

    # Roll the transaction back and hold it right after it is marked, before any stamp is cleared.
    # `KILL TRANSACTION` reaches that point without taking the background mutex the tasks hold.
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP_ROLLBACK"
    $CLICKHOUSE_CLIENT -q "kill transaction where tid=$tid format Null" &
    local kill_pid=$!
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP_ROLLBACK PAUSE"

    # Let the lookups finish: they now see the rollback and blank the stamps.
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_SLOW"
    sleep 4
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_ROLLBACK"
    wait $kill_pid 2>/dev/null
    tx_sync "$tx" "rollback" >/dev/null 2>&1
}

# A committed part that is not active and carries no removal stamp is the state under test. Parts
# created by the rolled-back transaction are excluded by their `creation_csn`.
count_not_restored() {
    $CLICKHOUSE_CLIENT -q "
        select count() from system.parts
        where database = currentDatabase() and table = 'mt'
          and active = 0
          and creation_csn not in (0, 18446744073709551615)
          and removal_tid = tuple(0, 0, toUUID('00000000-0000-0000-0000-000000000000'), 0)"
}

worst=0
for attempt in 1 2 3; do
    poison "$attempt"
    n=$(count_not_restored)
    if [ "$n" -gt "$worst" ]; then worst=$n; fi
done
echo -e "not_restored\t$worst"

# Every source is back in the Active set, so the merge retires all of them and the row appears once.
tx 8 "begin transaction"
tx 8 "optimize table mt final"
tx 8 "select 'rows', n from mt order by n"

# tx 8 holds the merge uncommitted, so every source is locked and the conflict must be reported at
# once. A part with no lock reports nothing and the wait runs to `max_execution_time`.
tx_short 9 "begin transaction" >/dev/null
tx_short 9 "alter table mt update n = 0 where 1" | grep -Eo "Serialization error|TIMEOUT_EXCEEDED" | uniq
tx_short 9 "rollback" >/dev/null
tx 8 "rollback"

$CLICKHOUSE_CLIENT -q "drop table mt"

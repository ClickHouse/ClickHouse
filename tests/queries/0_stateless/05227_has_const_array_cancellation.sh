#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `set` skip index evaluates its condition as an ExpressionActions over the values the index stored, so a
# `has()` over a constant array scans that array once per stored value inside one function call. That loop had
# no cancellation checkpoint, so neither `max_execution_time` nor `KILL QUERY` could stop the query while it
# ran; the reported query kept working for 182 seconds after its cancellation flag was already set.
#
# `timeout_overflow_mode = 'break'` is what makes the deadline oracle exact instead of a timing threshold: in
# break mode `QueryStatus::checkTimeLimit()` returns false rather than throwing, and every pre-existing call
# site in the index path discards that bool, so the checkpoint added here is the only code that can raise an
# error, and its message names the function. Without it the query raises nothing at all and returns a count.
#
# Exactly one active part is required, not merely tidy: cancellation is already checked once per (part, index)
# before the work, so with two or more parts those checks interrupt the query on their own and the test would
# pass without the fix.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_set_idx
    (
        type UInt32,
        uid LowCardinality(String),
        INDEX idx_uid uid TYPE set(10000) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY type
    -- pinned in the DDL: a granularity above the set's 10000 capacity would store far fewer values than
    -- there are rows, and it is one stored value per row that makes the scan long
    SETTINGS index_granularity = 1024;

    INSERT INTO t_set_idx SELECT 100500, toString(number % 10000) FROM numbers(200000);
    OPTIMIZE TABLE t_set_idx FINAL;
"

echo "active parts: $($CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_set_idx' AND active")"

# 50000 constant needles, none of them a uid value, so every stored value is compared against all of them:
# ~1e10 comparisons, about a minute even on a release build, so a 3s deadline always lands inside has().
NEEDLES="arrayMap(x -> toString(x), range(1000000, 1050000))"

# $1 = use_skip_indexes_on_data_read (1 = evaluated while reading, 0 = evaluated during planning; the two
# routes resolve the query differently, so both are checked), $2 = label, $3 = extra settings appended to the
# SETTINGS clause verbatim, leading comma included
deadline() {
    local qid="${CLICKHOUSE_DATABASE}_deadline_$1"
    if timeout 60 $CLICKHOUSE_CLIENT --query_id "$qid" --query "
            SELECT count() FROM t_set_idx WHERE has($NEEDLES, uid)
            SETTINGS use_skip_indexes = 1,               -- the defect is in skip index condition evaluation
                     use_skip_indexes_on_data_read = $1,
                     optimize_rewrite_has_to_in = 0,     -- keep the linear has(), do not rewrite it to a Set
                     max_execution_time = 3, timeout_overflow_mode = 'break'$3" 2>&1 \
        | grep -q "elapsed time limit reached in function has"
    then
        echo "$2: stopped in function has"
    else
        echo "$2: NOT stopped"
    fi

    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    # The time has to have been burned in skip index filtering rather than in the data path, otherwise the
    # test would keep passing while covering something else entirely.
    echo "$2 spent over 1s filtering marks: $($CLICKHOUSE_CLIENT -q "
        SELECT max(ProfileEvents['FilteringMarksWithSecondaryKeysMicroseconds']) > 1000000
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$qid' AND type != 'QueryStart'")"
}

# Bulk filtering evaluates a whole part in one condition call, so this scenario needs the periodic checkpoint
# and cannot be satisfied by the entry one; left unpinned, the randomizer picks the per-granule arm, where a
# later granule's entry check would already raise the error, on about half of the runs.
deadline 1 "read-time route" ", secondary_indices_enable_bulk_filtering = 1"
# Deliberately free in that dimension, which is what keeps the per-granule arm covered.
deadline 0 "planning route" ""

# The reported symptom: KILL QUERY does not stop it. No deadline here, so only the kill can end the query.
KILL_QID="${CLICKHOUSE_DATABASE}_kill"
KILL_OUT="${CLICKHOUSE_TMP}/killed_query.out"

$CLICKHOUSE_CLIENT --query_id "$KILL_QID" --query "
    SELECT count() FROM t_set_idx WHERE has($NEEDLES, uid)
    -- Unlike the deadline scenarios above, this one's oracle is a bound, so its cost must not depend on
    -- settings randomization: without bulk filtering the same scan takes 14.5s instead of 70s unfixed,
    -- and the query condition cache would serve it the verdict the two queries above already computed.
    SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1,
             secondary_indices_enable_bulk_filtering = 1, use_query_condition_cache = 0,
             optimize_rewrite_has_to_in = 0" > "$KILL_OUT" 2>&1 &
query_pid=$!

# Waiting for the query to be merely visible is not enough: the process list entry is inserted before planning
# starts, and a kill that lands in that window ends the query with the same QUERY_WAS_CANCELLED the oracle
# below greps for, without any of the work having run. One second of a scan that takes a minute is far past
# planning, so require elapsed time rather than existence.
for _ in {1..100}; do
    [ "$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.processes WHERE query_id = '$KILL_QID' AND elapsed > 1")" = "1" ] \
        && break
    sleep 0.2
done

if timeout 15 $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$KILL_QID' SYNC" > /dev/null 2>&1; then
    wait "$query_pid"
    if grep -q "QUERY_WAS_CANCELLED" "$KILL_OUT"; then
        echo "KILL QUERY: cancelled"
    else
        echo "KILL QUERY: finished without being cancelled"
    fi
else
    wait "$query_pid"
    echo "KILL QUERY: still waiting after 15s"
fi

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
# A liveness guard, not an oracle: it reads 1 whether or not the fix is present, and its job is to reject a run
# in which the kill ended the query before it entered index filtering, where the line above would still say
# "cancelled". The pre-existing per-index cancellation check returns before the timer starts, so zero here
# means the scan was never entered.
echo "KILL QUERY reached the index scan: $($CLICKHOUSE_CLIENT -q "
    SELECT max(ProfileEvents['FilteringMarksWithSecondaryKeysMicroseconds']) > 100000
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$KILL_QID' AND type != 'QueryStart'")"

rm -f "$KILL_OUT"

#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# Tag no-parallel: mt_background_executor_pretend_busy and replicated_queue_fail_next_entry are
#   server-global failpoints (the first one refuses every background task while it is enabled), and
#   the test asserts a server-wide ProfileEvent delta.
# Tag no-shared-merge-tree: SharedMergeTree does not use this scheduling path.
# Tag no-replicated-database: additional replicas or shards change the queue contents.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

trap '$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_background_executor_pretend_busy;
    SYSTEM DISABLE FAILPOINT replicated_queue_fail_next_entry;
    SYSTEM DISABLE FAILPOINT replicated_queue_unfail_entries;
" 2>/dev/null || true' EXIT

# Scheduling rejections the server has recorded so far; 0 before the first one.
rejections() {
    $CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM system.events WHERE event = 'ReplicatedQueueScheduleRejections'"
}

# "num_tries last_attempt_time entry_count" of the single queue entry under test. The timestamp is
# printed as a Unix time so that all three fields are space-free and word splitting is safe.
queue_state() {
    $CLICKHOUSE_CLIENT --query "
        SELECT max(num_tries), toUnixTimestamp(max(last_attempt_time)), count()
        FROM system.replication_queue
        WHERE database = currentDatabase() AND table = 'rq_reject'"
}

# 75 s, against ~13 s actually needed: the refusals are paced by the background assignee's own
# geometric backoff, which is wall-clock and not machine speed, so the margin is generous.
wait_for_rejections() {
    local target=$1
    for _ in {1..250}; do
        if [[ "$(rejections)" -ge "$target" ]]; then return 0; fi
        sleep 0.3
    done
    echo "TIMEOUT waiting for $target rejections, have $(rejections)"
    return 1
}

# max_postpone_time_for_failed_replicated_tasks_ms caps the entry's own retry delay at 100 ms, so the
# refusals below are paced by the background assignee alone and the test cannot be starved by the
# 2^num_tries backoff it is measuring.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS rq_reject SYNC;

    SET insert_keeper_fault_injection_probability = 0;

    CREATE TABLE rq_reject (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rq_reject', 'r1')
    ORDER BY x
    SETTINGS
        allow_remote_fs_zero_copy_replication = 0,
        max_postpone_time_for_failed_replicated_tasks_ms = 100;

    INSERT INTO rq_reject SELECT number FROM numbers(10);
"

# A permanently failing ALTER_METADATA entry: it is dispatched through the common pool and it does
# reach executeLogEntry, where the fault injection marks it once and then keeps throwing.
# alter_sync = 0 is mandatory: the default 1 would make the ALTER wait for the entry it just poisoned.
$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT replicated_queue_fail_next_entry;
    ALTER TABLE rq_reject ADD COLUMN y UInt8 DEFAULT 0 SETTINGS alter_sync = 0;
"

# Positive control: a real, failed attempt must advance num_tries. Without this a build that lost
# counting altogether would pass the differential below vacuously.
counted=0
for _ in {1..200}; do
    if [[ "$($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.replication_queue
            WHERE database = currentDatabase() AND table = 'rq_reject'
              AND num_tries >= 1 AND last_exception != ''")" == "1" ]]; then
        counted=1
        break
    fi
    sleep 0.3
done
if [[ "$counted" != "1" ]]; then
    echo "FAIL no real attempt was counted"
    exit 1
fi
echo "counting_still_works"

# The counters of that real attempt, sampled while every refusal is still impossible. The differential
# below cannot see a roll-back that clears the counters instead of undoing one attempt, because the
# first refusals happen before its own baseline is taken, so the baseline is anchored here as well.
read -r n0 t0 _ <<< "$(queue_state)"

# From here on the executor refuses every task, so no attempt can actually happen.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT mt_background_executor_pretend_busy;"

# Enter the refuse-only regime before measuring: these three rejections prove refusals are really
# happening, and they also mean any attempt still in flight above has finished.
wait_for_rejections "$(( $(rejections) + 3 ))"

read -r n1 t1 _ <<< "$(queue_state)"
e1=$(rejections)

# At least 10 further refusals, and by construction not a single real attempt, in this window.
wait_for_rejections "$(( e1 + 10 ))"

read -r n2 t2 cnt2 <<< "$(queue_state)"

if [[ "$cnt2" == "1" && "$n1" -ge "$n0" && "$t1" -ge "$t0" && "$n2" == "$n1" && "$t2" == "$t1" ]]; then
    echo "num_tries_stable_under_rejection"
else
    echo "FAIL entries=$cnt2 num_tries $n0 -> $n1 -> $n2, last_attempt_time $t0 -> $t1 -> $t2"
fi

# The entry really was executable, so a frozen counter above means "refused", not "never selectable".
# Un-fail before un-busying, or the first real attempt throws again and the drain wait flaps.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT replicated_queue_unfail_entries;"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_background_executor_pretend_busy;"

drained=0
for _ in {1..200}; do
    if [[ "$($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.replication_queue
            WHERE database = currentDatabase() AND table = 'rq_reject'")" == "0" ]]; then
        drained=1
        break
    fi
    sleep 0.3
done
if [[ "$drained" != "1" ]]; then
    echo "FAIL queue did not drain once the executor accepted tasks again"
fi

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT replicated_queue_unfail_entries;
    SYSTEM DISABLE FAILPOINT replicated_queue_fail_next_entry;
    DROP TABLE rq_reject SYNC;
"

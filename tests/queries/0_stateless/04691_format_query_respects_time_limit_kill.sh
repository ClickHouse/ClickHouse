#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs enough rows of moderate SQL text that an unpatched parse loop outlives the KILL.

# Companion to 04691_format_query_respects_time_limit.sql, which covers the `max_execution_time` half of
# the contract. KILL takes a different path (`is_killed` plus `throwProperExceptionIfNeeded`) than the
# timeout (`ExecutionSpeedLimits::checkTimeLimit`), so timeout coverage does not cover it: before the fix
# a `KILL QUERY ... SYNC` on one of these queries waited about 21 s for the block to run out.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_ID="04691_kill_${CLICKHOUSE_DATABASE}"

# No max_execution_time, so only the KILL can stop this. max_block_size / max_threads are pinned because
# the test runner randomizes both, and a small block would let an unpatched server finish its
# uninterruptible unit quickly enough to stay under the bound asserted below. The rows carry a trailing
# comment for the reason the `.sql` companion gives: the poll is throttled on input bytes while a row's
# parse cost is per AST node, so the KILL went unobserved for one stride, 19 s on MSan when unpadded.
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" --max_execution_time 0 -q "
    SELECT sum(length(formatQuery('SELECT ' || toString(number) || ' WHERE x=0' || repeat(' OR (y = 1)', 40) || ' -- ' || repeat('c', 2000))))
    FROM numbers(100000) FORMAT Null SETTINGS max_block_size = 200000, max_threads = 1
" &>/dev/null &

# Readiness waits for the query to have been running rather than merely being visible: `ProcessList`
# makes it visible before the executor is attached, and `addPipelineExecutor` raises a pending
# cancellation itself, so a kill winning that race would return promptly even unfixed. `max(elapsed)`
# over an empty set is 0, so this is false both while the query is absent and while it is only
# pending. Bounded by wall clock rather than an iteration count.
running=0
deadline=$((SECONDS + 60))
while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT max(elapsed) > 1 FROM system.processes WHERE query_id = '$QUERY_ID'")" == "1" ]; then
        running=1
        break
    fi
    sleep 0.2
done

# Without this the test could pass blind: a KILL that matches nothing is a silent no-op that returns
# immediately, so the latency bound below would report success having killed no query at all.
if [ "$running" != "1" ]; then
    echo "query never reached the row loop"
    wait || true
    exit 0
fi

started=$SECONDS
killed=$($CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC" 2>/dev/null | wc -l)
elapsed=$((SECONDS - started))

# 15 s matches the .sql test's threshold: about 21 s before the fix, about 0 s after it. The row count
# guards the same way as the wait above: KILL must report having acted on exactly one query.
if [ "$killed" != "1" ]; then
    echo "KILL reported $killed killed queries, expected 1"
elif [ "$elapsed" -lt 15 ]; then
    echo "killed promptly"
else
    echo "KILL took ${elapsed}s, expected under 15s"
fi

# The killed query's client exits non-zero, which is the expected outcome here.
wait || true

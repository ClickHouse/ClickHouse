#!/usr/bin/env bash
# Regression test: a background async-insert flush must honor `max_execution_time` while it is still
# parsing ONE block. The format is built with `max_insert_block_size`, so a single `read` call can run
# for minutes and the per-chunk check in `StreamingFormatExecutor::execute` is not reached until it
# returns.
#
# Two row loops carry that invariant on this path, and each one is checked separately below:
#   - `IRowInputFormat::read`        (row-based formats; a `JSON` column parsed from `TSV` here)
#   - `ValuesBlockInputFormat::read` (a loop of its own, it does not derive from `IRowInputFormat`)
#
# The deadline is the flush's own: `ProcessListBase::insert` registers every async-insert flush with
# `CancellationChecker` using the settings the flush inherited, so nothing here is shared with any
# other query on the server. That is why this test needs no failpoint, no `KILL QUERY` and no
# `no-parallel` tag - concurrent copies cannot perturb each other.
#
# What separates a fixed build from an unfixed one is the error code the FLUSH ITSELF recorded, not a
# timing margin and not text scraped from the client:
#   fixed   - the row loop polls the flush's `QueryStatus` mid-block and the deadline surfaces there,
#             so the flush fails with `TIMEOUT_EXCEEDED` (159).
#   unfixed - the loop has no checkpoint, so it parses the whole payload first and the deadline is
#             only observed by the executor between chunks, which reports `QUERY_WAS_CANCELLED` (394).
#             With a real payload that wait is minutes long and trips the stress hung check.
#
# The oracle is the flush's own `system.query_log` row, selected by `query_kind = 'AsyncInsertFlush'`,
# because only that row can say WHICH query hit its deadline and WHERE:
#   - the initiating `INSERT` inherits the same `max_execution_time`, and when its deadline wins it
#     reports a byte-identical `Timeout exceeded` of its own, so the client's stderr cannot tell an
#     in-loop abort from an initiator-side one and an unfixed build could satisfy a text assertion;
#   - a `KILL` from elsewhere yields `CANCELLED_BY_USER`, which this oracle names instead of silently
#     matching nothing, and each case is retried past it (see `MAX_ATTEMPTS`). Stress's own random
#     killer cannot reach this flush: `KILLER_EXCLUSION` puts a token matching one of that killer's
#     exclusions into the flush's query text.
#
# Companion of `04547_async_insert_flush_cancellation`, which pins the complementary case: a
# cancellation observed by the executor BETWEEN chunks. How `StreamingFormatExecutor::execute` then
# classifies the exception (rethrow versus route to `on_error`) is covered per code path by the unit
# test `StreamingFormatExecutorCancellation.*`, and the checkpoint together with its
# `CANCELLATION_CHECK_PERIOD_ROWS` period is pinned by `RowInputFormatCancellation.*`; both use no
# server and no global state.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Every table this run creates carries this prefix, and `run_case` appends the case label and the
# attempt number so that no two inserts of the run share a name. The `${CLICKHOUSE_DATABASE}`
# component keeps concurrent copies of the test non-interfering, and `$$` makes the name unique per
# INVOCATION as well: two invocations can land in the SAME database (Stress passes a fixed
# `--database` to half of its threads, and the runner re-runs a FAILED test in the database it already
# used), `DROP TABLE` does not delete `system.query_log` rows, and the oracle below matches on the
# table name - so without `$$` a second invocation that left no row of its own would read the first
# one's verdict and pass vacuously.
TABLE_PREFIX="t_flush_cancel_in_block_${CLICKHOUSE_DATABASE}_$$"
CLIENT_OUT="${CLICKHOUSE_TMP}/flush_cancel_in_block_$$.out"
PAYLOAD="${CLICKHOUSE_TMP}/flush_cancel_in_block_$$.data"

# The Stress runner injects `ignore_drop_queries_probability=0.2` into every client invocation, which
# makes an unpinned `DROP` report success without dropping. Every `DROP` whose effect this test
# depends on is pinned to 0 with statement-level `SETTINGS`, so that only the drop is exempted and
# anything else running in the same client session keeps the injected value.
DROP_SETTINGS="SETTINGS ignore_drop_queries_probability = 0"

function cleanup()
{
    # The KILL excludes ITSELF by `query_id`, not by the text `KILL QUERY`: the flush deliberately
    # carries that text (see `KILLER_EXCLUSION` below), so a text exclusion would skip exactly the
    # query this is meant to kill and leak a running flush.
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query LIKE '%$TABLE_PREFIX%' AND query_id != queryID() SYNC FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
    # One table per case per attempt, so enumerate them from `system.tables` by the shared prefix
    # instead of repeating `run_case`'s naming here.
    local leftovers
    leftovers=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '${TABLE_PREFIX}%'" 2>/dev/null ||:)
    local leftover
    for leftover in $leftovers; do
        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $leftover $DROP_SETTINGS" 2>/dev/null ||:
    done
    rm -f "$CLIENT_OUT" "$PAYLOAD"
}
trap cleanup EXIT

# One deadline for both cases, chosen from measured parse times (debug build, `ENGINE = Null` so the
# figure is parsing alone): the `TSV`/`JSON` payload below parses in ~1.2 s and the `Values` one in
# ~2.3 s, and an official release build is only modestly faster (~1.0 s / ~1.7 s). 500 ms therefore
# lands inside the single block in both cases with a wide margin, and stays above the 100 ms grid that
# `CancellationChecker` aligns deadlines to.
MAX_EXECUTION_TIME=0.5

# `ENGINE = Null`: this test is about aborting the PARSE, and a `MergeTree` write would add ~1 s of
# unrelated work to the flush - work that happens after parsing and so cannot be what the deadline
# interrupts.
#
# The URL settings force ONE block for the whole payload, which is the phase this fix is about:
#   - `min_insert_block_size_*` = 0 and `max_insert_block_size` huge: the row loop does not stop
#     early, so a single `read` call has to parse everything.
#   - `async_insert_max_data_size` large: the payload is buffered and flushed as one entry rather than
#     being sent synchronously (`PushResult::TOO_MUCH_DATA`).
#   - the busy-timeout pair: flush promptly instead of waiting out the queue timer.
#   - `timeout_overflow_mode = throw` is pinned explicitly: `'break'` routes the deadline to
#     `checkTimeLimit` instead of `cancelQuery`, and the runner randomizes settings.
BLOCK_SETTINGS="min_insert_block_size_rows=0&min_insert_block_size_bytes=0&max_insert_block_size=100000000&max_insert_block_size_bytes=0&input_format_max_block_size_bytes=0"
FLUSH_SETTINGS="async_insert=1&wait_for_async_insert=1&async_insert_busy_timeout_min_ms=10&async_insert_busy_timeout_max_ms=10&async_insert_max_data_size=2000000000"

# Stress runs a background killer that picks a random victim from `system.processes` filtered only by
# `query NOT LIKE '%system.processes%'`, `query NOT LIKE '%KILL QUERY%'` and `elapsed > 0.1`
# (`ci/jobs/scripts/stress/stress.py`, `RandomQueryKiller`). The flush is registered under its own
# serialized query text, so putting the literal `KILL QUERY` INSIDE that text makes it structurally
# ineligible instead of relying on the retry to win three coin flips. `ASTInsertQuery::formatImpl`
# formats `settings_ast` into the text, so a query-level `SETTINGS` clause is what survives
# serialization, and `log_comment` is a plain String setting with no execution semantics. The token
# sits in a string LITERAL, so the statement is an `INSERT` and never a `KILL QUERY`.
#
# Deliberately NOT the killer's other exclusion, `system.processes`: the runner's own hung check
# filters on that same text (`get_processlist_size` in `tests/clickhouse-test`), so that token would
# hide this flush from the very check this test exists to protect.
KILLER_EXCLUSION="SETTINGS+log_comment%3D%2704652+not-a-KILL+QUERY%2C+excluded+from+the+stress+random+killer%27"

# Two labels are transient rather than a verdict, and a retry is the only way to tell them from the
# unfixed abort:
#   - `cancelled-elsewhere`: some other `KILL` reached this flush. `KILLER_EXCLUSION` rules out the
#     Stress random killer, so this is the second line of defence, not the first.
#   - `between-chunks` when the deadline was already spent by the SETUP phase (it is armed at process
#     list registration, before the format and the executor are built). That aborts at the executor's
#     pre-chunk check, which throws from the same line with the same message as the unfixed mid-block
#     abort, so text cannot separate the two.
# Retrying cannot mask the bug: an unfixed build has no checkpoint in the row loop, so EVERY attempt
# parses the whole payload and yields `between-chunks`, and exhausting the attempts prints the last
# observed label rather than a synthesised pass.
MAX_ATTEMPTS=3

# $1 - label, also used as the `FORMAT` clause. $2 - the column type.
function run_case()
{
    local format="$1"
    local column_type="$2"
    echo "$format"

    local case_id
    case_id=$(echo "$format" | tr '[:upper:]' '[:lower:]')

    local attempt
    local label=""
    local table
    for ((attempt = 1; attempt <= MAX_ATTEMPTS; ++attempt)); do
        # One table per case AND per attempt: the oracle below narrows by table name, so a name shared
        # with another case or another attempt would let its `system.query_log` row satisfy every
        # predicate and be returned when THIS flush left no row of its own.
        table="${TABLE_PREFIX}_${case_id}_${attempt}"

        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table $DROP_SETTINGS"
        $CLICKHOUSE_CLIENT -q "CREATE TABLE $table (x $column_type) ENGINE = Null"

        # Synchronous async insert over HTTP: the request waits for the background flush, so it
        # observes the exception the flush aborts with. The flush inherits these settings verbatim,
        # which is how it gets its own deadline.
        ${CLICKHOUSE_CURL} -sS \
            "${CLICKHOUSE_URL}&${FLUSH_SETTINGS}&${BLOCK_SETTINGS}&max_execution_time=${MAX_EXECUTION_TIME}&timeout_overflow_mode=throw&query=INSERT+INTO+${table}+${KILLER_EXCLUSION}+FORMAT+${format}" \
            --data-binary @"$PAYLOAD" > "$CLIENT_OUT" 2>&1 ||:

        # Read the verdict off the flush's own `system.query_log` row rather than the client's stderr.
        #   `query_kind = 'AsyncInsertFlush'` - excludes the initiating `INSERT`, which carries the
        #                                      same deadline and the same message when IT times out
        #                                      first.
        #   `has(databases, currentDatabase())` - scopes the row to this test's database. NOTE the
        #                                      flush runs on a background thread whose
        #                                      `current_database` is `default`, not the test database,
        #                                      so the usual `current_database = currentDatabase()`
        #                                      predicate matches NOTHING here; `databases` does carry
        #                                      the target's database.
        #   `query LIKE '%$table%'`          - the table name is unique per invocation, per case and
        #                                      per attempt, and it is embedded in `query_for_logging`,
        #                                      so it names THIS flush and no other. That is what makes
        #                                      an absent row print nothing - and therefore FAIL -
        #                                      instead of inheriting another invocation's, case's or
        #                                      attempt's verdict. See `TABLE_PREFIX` for why the
        #                                      invocation component is what closes the stale-row hole;
        #                                      the `event_date` bound below is only a cheap partition
        #                                      prune, it is not what keeps the row fresh.
        #   `type != 'QueryStart'`           - `QueryStart` has no `exception_code` yet.
        #   `ORDER BY ... DESC LIMIT 1`      - newest row only.
        label=$($CLICKHOUSE_CLIENT -q "
            SYSTEM FLUSH LOGS query_log;
            SELECT multiIf(
                       exception_code = 159, 'in-loop-timeout',
                       exception_code = 394 AND exception ILIKE '%Format streaming was cancelled%', 'between-chunks',
                       exception_code = 394, 'cancelled-elsewhere',
                       'other-' || toString(exception_code))
            FROM system.query_log
            WHERE query_kind = 'AsyncInsertFlush'
              AND has(databases, currentDatabase())
              AND query LIKE '%$table%'
              AND type != 'QueryStart'
              AND event_date >= today() - 1
            ORDER BY event_time_microseconds DESC
            LIMIT 1")

        # Only the two transient labels are retried. Anything else - including the empty string, which
        # means this flush left no row - is final and printed as it is.
        if [ "$label" != "cancelled-elsewhere" ] && [ "$label" != "between-chunks" ]; then
            break
        fi
    done

    echo "$label"
}

# Row-based format: IRowInputFormat::read. A `JSON` column gives the loop enough work per row to
# outlive the deadline from a payload of a few megabytes; a plain integer column parses ~100x faster
# per byte and would need a payload too large for a stateless test. It is also the shape the original
# hung check reported, which was stuck in `JSON` deserialization.
python3 - "$PAYLOAD" <<'END_OF_PYTHON'
import json
import sys

with open(sys.argv[1], "w") as out:
    for i in range(30000):
        row = {"p{0}_{1}".format(i, j): {"n": i, "arr": [i, i + 1], "s": "v{0}".format(i)} for j in range(4)}
        out.write(json.dumps(row) + "\n")
END_OF_PYTHON
run_case TSV JSON

# `Values` has a row loop of its own, and `INSERT ... VALUES` is the commonest async-insert shape. The
# rows vary in expression STRUCTURE on purpose: a uniform payload is served by the template cache,
# and only the expression fallback is slow enough to outlive the deadline at this size.
python3 - "$PAYLOAD" <<'END_OF_PYTHON'
import sys

shapes = [
    "({0} + 1)",
    "({0} * 2 - 1)",
    "(cityHash64(toString({0})) % 97)",
    "(if({0} % 2 = 0, 1, 2))",
    "(length(concat('a', '{0}')))",
    "(toUInt64(abs(-{0})))",
    "({0})",
]
with open(sys.argv[1], "w") as out:
    for i in range(100000):
        out.write(shapes[i % len(shapes)].format(i) + "\n")
END_OF_PYTHON
run_case Values UInt64

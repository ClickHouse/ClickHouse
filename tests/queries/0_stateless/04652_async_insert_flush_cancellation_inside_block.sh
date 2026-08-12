#!/usr/bin/env bash
# A background async-insert flush must honour `max_execution_time` while still parsing ONE block.
# One case per row loop, neither deriving from the other. Companion of
# `04547_async_insert_flush_cancellation`, which pins the between-chunks case.
#
# The verdict is the code the FLUSH ITSELF recorded, read off its own `system.query_log` row and not
# the client's stderr: `TIMEOUT_EXCEEDED` (159) when the row loop polled mid-block, else (394).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# `$$` is load-bearing, not decoration: two invocations can land in the same database, `DROP TABLE`
# does not delete `system.query_log` rows, and the oracle below matches on the table name - so
# without it an invocation that left no row of its own would read an earlier one's verdict.
TABLE_PREFIX="t_flush_cancel_in_block_${CLICKHOUSE_DATABASE}_$$"
CLIENT_OUT="${CLICKHOUSE_TMP}/flush_cancel_in_block_$$.out"
PAYLOAD="${CLICKHOUSE_TMP}/flush_cancel_in_block_$$.data"

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
        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $leftover" 2>/dev/null ||:
    done
    rm -f "$CLIENT_OUT" "$PAYLOAD"
}
trap cleanup EXIT

# Measured parse times are ~1.2 s (`TSV`) and ~2.3 s (`Values`) on a debug build, ~1.0 s / ~1.7 s on
# a release one, so 500 ms lands inside the single block either way. It also stays above the 100 ms
# grid that `CancellationChecker` aligns deadlines to.
MAX_EXECUTION_TIME=0.5

# `ENGINE = Null` so that a `MergeTree` write does not add ~1 s of post-parse work. The settings
# below force ONE block for the whole payload; `timeout_overflow_mode` is pinned because `'break'`
# routes the deadline to `checkTimeLimit` instead of `cancelQuery` and the runner randomizes it.
BLOCK_SETTINGS="min_insert_block_size_rows=0&min_insert_block_size_bytes=0&max_insert_block_size=100000000&max_insert_block_size_bytes=0&input_format_max_block_size_bytes=0"
FLUSH_SETTINGS="async_insert=1&wait_for_async_insert=1&async_insert_busy_timeout_min_ms=10&async_insert_busy_timeout_max_ms=10&async_insert_max_data_size=2000000000"

# Stress's `RandomQueryKiller` skips any query whose text matches `KILL QUERY`, so carrying that
# token in a string literal makes this flush structurally ineligible. NOT its other exclusion,
# `system.processes`: the runner's own hung check filters on that text and would stop seeing us.
KILLER_EXCLUSION="SETTINGS+log_comment%3D%2704652+not-a-KILL+QUERY%2C+excluded+from+the+stress+random+killer%27"

# `cancelled-elsewhere`, `between-chunks` and the empty string are transient, not verdicts, so they
# are retried. Retrying cannot mask the bug: an unfixed build yields `between-chunks` on EVERY
# attempt, and exhausting the attempts prints the last observed label rather than a synthesised pass.
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

        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table"
        $CLICKHOUSE_CLIENT -q "CREATE TABLE $table (x $column_type) ENGINE = Null"

        # Synchronous async insert over HTTP: the request waits for the background flush, so it
        # observes the exception the flush aborts with. The flush inherits these settings verbatim,
        # which is how it gets its own deadline.
        ${CLICKHOUSE_CURL} -sS \
            "${CLICKHOUSE_URL}&${FLUSH_SETTINGS}&${BLOCK_SETTINGS}&max_execution_time=${MAX_EXECUTION_TIME}&timeout_overflow_mode=throw&query=INSERT+INTO+${table}+${KILLER_EXCLUSION}+FORMAT+${format}" \
            --data-binary @"$PAYLOAD" > "$CLIENT_OUT" 2>&1 ||:

        # `has(databases, ...)` and not `current_database`: the flush runs on a background thread
        # whose `current_database` is `default`, so the usual predicate matches NOTHING here. The
        # table name is what names THIS flush.
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

        # An empty label means this flush left no row at all, so there is nothing to read a verdict
        # from: a lost sample rather than an outcome, hence retried like the other two.
        if [ "$label" != "cancelled-elsewhere" ] && [ "$label" != "between-chunks" ] && [ -n "$label" ]; then
            break
        fi
    done

    # Name the exhausted-retries outcome; a blank line is indistinguishable from truncated stdout.
    echo "${label:-no-flush-row}"
}

# `IRowInputFormat::read`. A `JSON` column gives the loop enough work per row to outlive the deadline
# from a few megabytes, where a plain integer would need a payload too large for a stateless test.
# It is also the shape the original hung check reported.
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

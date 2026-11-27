#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression for `StrictResizeProcessor::prepare`: when an input port finished, the output it had
# been paired with was released into `waiting_outputs` unconditionally - even while it still held a
# chunk that the downstream processor had not consumed yet (`OutputStatus::NotActive`), or while it
# was already queued there. That output was then handed out to a second input, and the next chunk
# arriving on that input hit `Invalid status NotActive for associated output`.
#
# The race needs a high thread count and inputs that finish early, hence the remote source with
# `WITH TOTALS` feeding a `GROUPING SETS` aggregation. It reproduced in about 3% of the runs.

QUERY="SELECT sum(number) FROM (SELECT number FROM remote('127.0.0.1', view(SELECT * FROM numbers_mt(10000000) GROUP BY ALL WITH TOTALS)) GROUP BY GROUPING SETS ((number)))"

# Every run returns the correct sum, and none of them fails with the logical error. The loop is
# additionally capped by wall-clock time: on a release build all 100 iterations finish within the
# budget (and reproduced the bug in ~3% of the runs before the fix), while under sanitizers or
# coverage instrumentation a single run takes tens of seconds and the full 100 iterations would
# not fit into the test timeout.
for _ in {1..100}
do
    RESULT=$($CLICKHOUSE_CLIENT --max_threads 49 --max_rows_to_read 0 -q "$QUERY" 2>&1)
    if [ "$RESULT" != "49999995000000" ]
    then
        echo "Unexpected result: $RESULT"
    fi
    if [ "$SECONDS" -ge 60 ]
    then
        break
    fi
done

echo "OK"

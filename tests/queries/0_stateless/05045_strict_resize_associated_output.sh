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

OUTPUT=$(for _ in {1..100}; do $CLICKHOUSE_CLIENT --max_threads 49 --max_rows_to_read 0 -q "$QUERY" 2>&1; done)

# Every run returns the correct sum, and none of them fails with the logical error.
echo "$OUTPUT" | { grep -cF '49999995000000' || true; }
echo "$OUTPUT" | { grep -cF 'Invalid status NotActive for associated output' || true; }

#!/usr/bin/env bash
# Tags: distributed, no-llvm-coverage
# no-llvm-coverage: the test needs the shard accumulating `repeat('a', 100000)` per row to hit
# `max_memory_usage=1G` and emit `Query memory limit exceeded` before the other shard cancels.
# LLVM source-based coverage instrumentation perturbs this memory-tracking cadence, so the
# expected exception sometimes does not surface on `127.3` before cancellation, leaving an empty
# `grep` match and a flaky FAIL.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# _shard_num:
#   1 on 127.2
#   2 on 127.3
# max_block_size to fail faster
# `groupArrayIf` on `_shard_num` makes `127.3` the only shard that can run out of memory: on
# `127.2` the condition is false for every row, so its aggregate states never grow and it just
# streams `system.numbers` until the initiator cancels it. Before this, `127.2` accumulated
# one-byte strings and reached the same `max_memory_usage` only about 20% later than `127.3`, so
# which shard failed first was a race.
opts=(
    "--max_memory_usage=1G"
    "--max_block_size=50"
    "--max_threads=1"
    "--max_distributed_connections=2"
    "--max_bytes_before_external_group_by=0"
    "--max_bytes_ratio_before_external_group_by=0"
    # The query reads `system.numbers` until a shard runs out of memory, so `max_memory_usage`
    # has to be the only limit that can stop it. The test profile of the CI configuration
    # (tests/config/users.d/limits.yaml) sets `max_rows_to_read = 20000000`, which `127.2` and
    # the initiator reach at about the same time as `127.3` reaches `max_memory_usage`; the query
    # then fails with `TOO_MANY_ROWS` instead of `MEMORY_LIMIT_EXCEEDED` and the `grep` below
    # finds nothing.
    "--max_rows_to_read=0"
    # `127.2` now never fails on its own, so if the cancellation this test is about ever breaks,
    # the query would stream forever and the test would be killed by the runner's timeout with no
    # output. Fail it with `TIMEOUT_EXCEEDED` instead, far above the ~1s the expected exception
    # needs.
    "--max_execution_time=100"
)
LOG="$CLICKHOUSE_TMP/err-$CLICKHOUSE_DATABASE"
trap 'rm -f "$LOG"' EXIT

${CLICKHOUSE_CLIENT} "${opts[@]}" -q "SELECT groupArrayIf(repeat('a', 100000), _shard_num == 2), number%100000 k from remote('127.{2,3}', system.numbers) GROUP BY k LIMIT 10e6" > "$LOG" 2>&1
CODE=$?

# the query should fail earlier on 127.3 and 127.2 should not even go to the memory limit exceeded error.
# while if this will not correctly then it will got the exception from the 127.2:9000 and fail
if ! grep -F -q "DB::Exception: Received from 127.3:${CLICKHOUSE_PORT_TCP}. DB::Exception: Query memory limit exceeded:" "$LOG"; then
    # stderr only on the failing path: a test that writes there and exits 0 is a FAIL for the runner.
    echo "Fail, Code: $CODE, client output:" >&2
    cat "$LOG" >&2
    exit 1
fi

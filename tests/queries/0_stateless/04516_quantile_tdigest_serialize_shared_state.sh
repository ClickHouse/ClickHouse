#!/usr/bin/env bash
# Tags: long
# Regression test: serializing a shared/const quantileTDigest state as a parallel GROUP BY key
# used to mutate the shared state via compress() (data race -> heap-buffer-overflow in RadixSort).
# The race is probabilistic per query run; the pre-fix binary trips ASan within the first 1-2
# iterations of this loop, so a short loop is enough for a reliable failure. The fixed serialize()
# is const and never touches the shared centroids buffer. Tagged `long` for the flaky check's 180s
# soft ceiling (the query is heavy on slow sanitizer builds); the tag does not raise the runner's
# 600s hard --timeout.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A single tdigest state (const, one row) broadcast across many rows and grouped by, so the
# Aggregator serializes the SAME shared state pointer from several threads concurrently.
# optimize_group_by_constant_keys is pinned: with it off the state is serialized once per row
# instead of once per block, an order of magnitude slower, which hit the 600s hard timeout under
# coverage. Threads still serialize the same shared state, so the race stays covered.
query="SELECT count()
FROM
(
    SELECT k
    FROM
    (
        SELECT number, (SELECT quantileTDigestState(number) FROM numbers(5000)) AS k
        FROM numbers_mt(200000)
    )
    GROUP BY k
) SETTINGS max_threads = 16, optimize_group_by_constant_keys = 1"

for _ in {1..10}; do
    ${CLICKHOUSE_CLIENT} --query "$query"
done

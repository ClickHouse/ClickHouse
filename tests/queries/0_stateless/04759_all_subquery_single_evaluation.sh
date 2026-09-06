#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `x op ALL (subquery)` is rewritten into an emptiness check (needed for vacuous truth) plus a comparison
# against an aggregate of the subquery. Both must come from a single evaluation of the right-hand side,
# otherwise a non-deterministic subquery can be observed in two different states.

# The expected result is 1 for every possible evaluation: either the subquery is empty, and then the answer is
# TRUE by vacuous truth, or its only row is 1, which is equal to the left-hand side. If the right-hand side
# were evaluated twice, the emptiness check and the comparison could disagree, and the query would return 0.
for _ in {1..100}; do
    echo "SELECT 1 = ALL (SELECT 1 FROM numbers(1) WHERE rand() % 2 = 0);"
done | $CLICKHOUSE_CLIENT | sort --unique

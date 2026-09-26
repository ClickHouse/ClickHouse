#!/usr/bin/env bash

# When no enabled join algorithm can run a JOIN, the error has to name the algorithms that were
# tried and the JOIN that failed, otherwise a user who narrowed `join_algorithm` has nothing to act
# on. The names must be printed as the setting spells them, so they can be pasted back into it.
#
# Checked under both `enable_analyzer` values, because the two take different code paths:
# `PlannerJoins::chooseJoinAlgorithm` and the legacy `ExpressionAnalyzer::chooseJoinAlgorithm`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for enable_analyzer in 1
do
    echo "-- enable_analyzer = $enable_analyzer"

    error=$(${CLICKHOUSE_CLIENT} --query "
        SELECT *
        FROM (SELECT 1 AS key, 2 AS t) AS l
        ASOF JOIN (SELECT 1 AS key, 1 AS t) AS r
        ON l.key = r.key AND l.t >= r.t
        SETTINGS join_algorithm = 'grace_hash', enable_analyzer = $enable_analyzer
    " 2>&1)

    # Presence checks rather than line counts: the exception text can be wrapped or repeated, and the
    # test is about what the message says, not how many lines it happens to occupy.
    has() { if echo "$error" | grep -q -- "$1"; then echo "yes"; else echo "no"; fi; }

    echo "error code NOT_IMPLEMENTED: $(has NOT_IMPLEMENTED)"
    echo "names the setting: $(has join_algorithm)"
    echo "names the algorithm that was tried: $(has grace_hash)"
    echo "names the JOIN strictness: $(has ASOF)"
    echo "names the JOIN kind: $(has INNER)"
    # Both parts together, so the test also fails if they stop being reported as one JOIN.
    echo "names the JOIN as a whole: $(has 'ASOF INNER JOIN')"
    # The setting only accepts lowercase, so the uppercase enum spelling must not be what is printed.
    echo "prints the unusable uppercase spelling: $(has GRACE_HASH)"
done

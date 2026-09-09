#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS l;
    DROP TABLE IF EXISTS r;
    CREATE TABLE l (a UInt32, t UInt32) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE r (a UInt32, t UInt32) ENGINE = MergeTree ORDER BY a;
    INSERT INTO l VALUES (1, 10), (1, 20);
    INSERT INTO r VALUES (1, 5), (1, 15);
"

run()
{
    echo "--- $1"
    ${CLICKHOUSE_CLIENT} -q "$1" 2>&1 | grep -m1 -oE 'Code: [0-9]+\. DB::Exception: .*' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' \
              -e 's/: While executing .*//' -e 's/: In scope .*//' -e 's/ (version .*//'
}

echo '=== the condition of an -If combinator is named as such'
# The blamed argument is a perfectly good argument of `sum`; the condition is simply missing.
run "SELECT sumIf(a) FROM l"
run "SELECT sumIf(a, t) FROM l"
run "SELECT avgIf(a) FROM l"
# `countIf` takes only the condition, so one argument is the right count there.
run "SELECT countIf(a) FROM l"

# The wording of these messages is produced by the analyzer, so pin it regardless of the default in the run.
echo
echo '=== an ASOF join says that the inequality is needed in addition to the equalities'
run "SELECT * FROM l ASOF JOIN r ON l.a = r.a SETTINGS enable_analyzer = 1"

echo
echo '=== equality predicates are optional, so they are not mentioned when there are none'
run "SELECT * FROM l ASOF JOIN r ON l.t != r.t SETTINGS enable_analyzer = 1"

echo
echo '=== the same, spelled correctly'
${CLICKHOUSE_CLIENT} -q "SELECT sumIf(a, t > 0) FROM l"
${CLICKHOUSE_CLIENT} -q "SELECT countIf(t > 0) FROM l"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM (SELECT * FROM l ASOF JOIN r ON l.a = r.a AND l.t >= r.t)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE l; DROP TABLE r"

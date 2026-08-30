#!/usr/bin/env bash
# Tags: no-old-analyzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

run()
{
    echo "--- $1"
    ${CLICKHOUSE_CLIENT} -q "$1" 2>&1 | grep -m1 -E '^Code: ' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: Received from [^ ]* DB::Exception: /Code: \1. /' \
              -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' \
              -e 's/: while executing .*//' -e 's/: In scope .*//' -e 's/ (version [^)]*)$//'
}

echo '=== an aggregate in WHERE names the clause that does accept it'
# The alias is expanded before the check, so without the hint the message is about an aggregate
# in WHERE that the user never wrote.
run "SELECT count() AS c FROM numbers(10) WHERE c > 1"
run "SELECT count() FROM numbers(10) WHERE count() > 1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t; CREATE TABLE t (x UInt32) ORDER BY x"
run "SELECT count() FROM t PREWHERE count() > 1"
# The old analyzer has its own copy of the check and now gives the same advice.
run "SELECT count() AS c FROM numbers(10) WHERE c > 1 SETTINGS enable_analyzer = 0"

# HAVING, which is what the hint suggests, works.
${CLICKHOUSE_CLIENT} -q "SELECT count() AS c FROM numbers(10) HAVING c > 1"

echo
echo '=== a value that is not a date is not reported as being too short'
run "SELECT toDate('yesterday')"
run "SELECT toDate('202')"
# 10 characters or more take the fast path, which shows the offending value instead.
run "SELECT toDate('abcd-01-02')"
# Delimiters other than `-` are accepted, which is why the message names the canonical format
# rather than claiming that a particular character is required.
${CLICKHOUSE_CLIENT} -q "SELECT toDate('2020!01!02')"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

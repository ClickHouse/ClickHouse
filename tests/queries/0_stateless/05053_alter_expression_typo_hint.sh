#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (id UInt32, x UInt32, s String, d Date) ENGINE = MergeTree ORDER BY id
"

# `SELECT ix FROM t` has always suggested `id`. The key, index and TTL expressions of ALTER are validated
# through a path that is given no storage to ask for hints, so they only listed the available columns -
# which for a MergeTree table starts with a dozen virtual columns and is cut off before the real ones.
run()
{
    echo "--- $1"
    ${CLICKHOUSE_CLIENT} -q "$1" 2>&1 | grep -m1 -oE 'Code: [0-9]+\. DB::Exception: .*' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' \
              -e 's/: While executing .*//' -e 's/ (version .*//'
}

echo '=== a typo in a key, index or TTL expression suggests the column'
run "ALTER TABLE t MODIFY ORDER BY (id, ix)"
run "ALTER TABLE t ADD INDEX i ix TYPE minmax GRANULARITY 1"
run "ALTER TABLE t MODIFY TTL ix + INTERVAL 1 DAY"

echo
echo '=== a name that is nothing like a column still gets the list of available columns'
run "ALTER TABLE t MODIFY TTL zzzzzzzzzzzz + INTERVAL 1 DAY"

echo
echo '=== a virtual column is not accepted in a sorting key expression, so it is never suggested'
# The sorting key is analyzed over the columns extended with the virtual ones, but an expression added to
# the sorting key may use only the columns added by the same ALTER, so a virtual column is rejected anyway.
run "ALTER TABLE t MODIFY ORDER BY (id, _part_indez)"
run "ALTER TABLE t MODIFY ORDER BY (id, zzzzzzzzzzzz)"

echo
echo '=== the same expressions, spelled correctly'
# The sorting key can only be extended with a column added by the same ALTER.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t ADD COLUMN n UInt32, MODIFY ORDER BY (id, n)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t ADD INDEX i x TYPE minmax GRANULARITY 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t MODIFY TTL d + INTERVAL 1 DAY"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

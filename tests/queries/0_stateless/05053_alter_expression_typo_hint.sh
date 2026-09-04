#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (id UInt32, x UInt32, s String, d Date, tp Tuple(a UInt32, b String)) ENGINE = MergeTree ORDER BY id
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

echo '=== a typo in an index or TTL expression suggests the column'
run "ALTER TABLE t ADD INDEX i ix TYPE minmax GRANULARITY 1"
run "ALTER TABLE t MODIFY TTL ix + INTERVAL 1 DAY"

echo
echo '=== a name that is nothing like a column still gets the list of available columns'
run "ALTER TABLE t MODIFY TTL zzzzzzzzzzzz + INTERVAL 1 DAY"

echo
echo '=== an expression added to the sorting key may use only the columns added by the same ALTER, so only they are suggested'
# `id` is as close to `idz` as `idx` is, but it exists already, so using it would only fail the next check.
run "ALTER TABLE t ADD COLUMN idx UInt32, MODIFY ORDER BY (id, idz)"
run "ALTER TABLE t ADD COLUMN n UInt32, MODIFY ORDER BY (id, idz)"
run "ALTER TABLE t MODIFY ORDER BY (id, idz)"
run "ALTER TABLE t MODIFY ORDER BY (id, _part_indez)"

echo
echo '=== a subcolumn is legal in a key, so it is suggested too'
run "ALTER TABLE t ADD COLUMN tp2 Tuple(a UInt32, b String), MODIFY ORDER BY (id, tp2.c)"
run "CREATE TABLE t2 (id UInt32, tp Tuple(a UInt32, b String)) ENGINE = MergeTree ORDER BY (id, tp.c)"

echo
echo '=== the same expressions, spelled correctly'
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t ADD INDEX i x TYPE minmax GRANULARITY 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t MODIFY TTL d + INTERVAL 1 DAY"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t ADD COLUMN idx UInt32, MODIFY ORDER BY (id, idx)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t ADD COLUMN tp2 Tuple(a UInt32, b String), MODIFY ORDER BY (id, idx, tp2.a)"
${CLICKHOUSE_CLIENT} -q "SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

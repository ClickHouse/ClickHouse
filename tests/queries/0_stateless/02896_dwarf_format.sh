#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('$CURDIR/data_dwarf/a.out', DWARF) order by cityHash64(offset) limit 10"

# Select each column individually to make sure we didn't mess up any of the `if (need[COL_WHATEVER])` checks in the code.
for c in `$CLICKHOUSE_LOCAL -q "desc file('$CURDIR/data_dwarf/a.out', DWARF)" | cut -f1`
do
    $CLICKHOUSE_LOCAL -q "select '$c', sum(cityHash64($c) as h), argMin($c, h) as random_value_to_sanity_check_visually from file('$CURDIR/data_dwarf/a.out', DWARF)"
done

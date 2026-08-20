#!/usr/bin/env bash
# Tags: no-old-analyzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A block nested loop join streams its build side to a temporary file once the store holds more than
# `max_bytes_before_external_join`. What it writes is accounted as a join's external data, the way
# `GraceHashJoin` accounts for its own - the store opens a child temporary-data scope carrying the
# `ExternalJoin*` events, and without that scope the spill is invisible in them. Each build stream
# spills to a file of its own, which is what makes a spilled store a set of files rather than one.
#
# The events are read from the packets the server sends with the query rather than from the query
# log, so that the test does not wait for `SYSTEM FLUSH LOGS` - a server-wide flush that on a loaded
# runner takes far longer than the query.

settings="cross_to_inner_join_rewrite = 0, join_use_nulls = 0, join_algorithm = 'partial_merge',
    max_bytes_before_external_join = 1, max_block_size = 6, query_plan_join_swap_table = 'false'"

# A `UNION ALL` of two sources gives the build side rows to split between as many streams as the
# pipeline is allowed to run.
build="(SELECT number % 6 AS y FROM numbers(15) UNION ALL SELECT number % 6 AS y FROM numbers(15, 15))"

spill_events()
{
    $CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 --query "
        SELECT count() FROM (SELECT number % 9 AS x FROM numbers(40)) l
        FULL JOIN $build r ON l.x < r.y
        SETTINGS $settings, max_threads = $1
        FORMAT Null" 2>&1 >/dev/null |
        sed -n 's/^.*\] ExternalJoin\([A-Za-z]*\): \([0-9]*\) .*$/\1=\2/p'
}

event() { sed -n "s/^$1=//p" <<< "$2"; }

one_stream=$(spill_events 1)
two_streams=$(spill_events 2)

compressed=$(event CompressedBytes "$two_streams")
uncompressed=$(event UncompressedBytes "$two_streams")

echo -e "accounted as external join data\t$((compressed > 0))\t$((uncompressed > 0))"
echo -e "a file per build stream\t$(event WritePart "$one_stream")\t$(event WritePart "$two_streams")"

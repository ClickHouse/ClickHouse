#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SIZE=13
for OFFSET in {0..15}; do
    for LIMIT in {0..15}; do
        echo "SELECT
            $OFFSET,
            $LIMIT,
            count() AS c,
            min(number) AS first,
            max(number) AS last,
            throwIf(first != ($OFFSET < $SIZE AND $LIMIT > 0 ? $OFFSET : 0)),
            throwIf(last != ($OFFSET < $SIZE AND $LIMIT > 0 ? least($SIZE - 1, $OFFSET + $LIMIT - 1) : 0)),
            throwIf((c != 0 OR first != 0 OR last != 0) AND (c != last - first + 1))
            FROM (SELECT * FROM numbers($SIZE) LIMIT $OFFSET, $LIMIT);
        "
    done
done | $CLICKHOUSE_CLIENT --max_block_size 5

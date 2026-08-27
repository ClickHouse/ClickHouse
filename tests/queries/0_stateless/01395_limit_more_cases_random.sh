#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SIZE=13
ITERATIONS=300
for _ in $(seq $ITERATIONS); do
    SIZE=$(($RANDOM % 100))
    OFFSET=$(($RANDOM % 111))
    LIMIT=$(($RANDOM % 111))

    echo "WITH count() AS c, min(number) AS first, max(number) AS last
            SELECT
                throwIf(first != ($OFFSET < $SIZE AND $LIMIT > 0 ? $OFFSET : 0)),
                throwIf(last != ($OFFSET < $SIZE AND $LIMIT > 0 ? least($SIZE - 1, $OFFSET + $LIMIT - 1) : 0)),
                throwIf((c != 0 OR first != 0 OR last != 0) AND (c != last - first + 1))
            FROM (SELECT * FROM numbers($SIZE) LIMIT $OFFSET, $LIMIT);
        "
done | $CLICKHOUSE_CLIENT --max_block_size $(($RANDOM % 20 + 1)) | uniq

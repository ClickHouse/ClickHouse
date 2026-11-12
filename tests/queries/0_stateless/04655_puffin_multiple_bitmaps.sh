#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_puffin"

echo "--- multi_bitmap_keys.puffin ---"
$CLICKHOUSE_LOCAL -q "
SELECT
    length(deleted_rows),
    deleted_rows[1],
    deleted_rows[-1],
    arraySum(deleted_rows),
    deleted_rows = arraySort(deleted_rows),
    arrayDistinct(arrayMap(x -> bitShiftRight(x, 32), deleted_rows))
FROM file('$DATA/multi_bitmap_keys.puffin', Puffin)
"

echo "--- per key ---"
$CLICKHOUSE_LOCAL -q "
SELECT key, count(), min(position), max(position)
FROM
(
    SELECT bitShiftRight(position, 32) AS key, position
    FROM file('$DATA/multi_bitmap_keys.puffin', Puffin)
    ARRAY JOIN deleted_rows AS position
)
GROUP BY key
ORDER BY key
"

for name in bitmap_keys_out_of_order bitmap_keys_duplicate bitmap_count_exceeds_data
do
    echo "--- $name.puffin ---"
    $CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$DATA/$name.puffin', Puffin)" 2>&1 \
        | grep -oE "keys must be sorted in ascending order|truncated while reading key" || true
done

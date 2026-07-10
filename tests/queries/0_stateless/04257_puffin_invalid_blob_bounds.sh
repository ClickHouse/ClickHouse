#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_puffin"

for PUFFIN in \
    "$DATA/overflow_offset_length.puffin" \
    "$DATA/negative_offset.puffin" \
    "$DATA/length_exceeds_file.puffin"
do
    echo "--- $(basename "$PUFFIN") ---"
    $CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$PUFFIN', Puffin)" 2>&1 | grep -oF 'Puffin blob 0: offset/length out of bounds'
done

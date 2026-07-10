#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PUFFIN="$CURDIR/data_puffin/04261_invalid_bitmap_key.puffin"

echo "--- $(basename "$PUFFIN") ---"
$CLICKHOUSE_LOCAL -q "SELECT deleted_rows FROM file('$PUFFIN', Puffin)" 2>&1 | grep -oF 'Invalid deletion vector bitmap key'

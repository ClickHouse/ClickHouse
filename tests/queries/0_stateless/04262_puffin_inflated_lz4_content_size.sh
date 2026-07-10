#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PUFFIN="$CURDIR/data_puffin/04262_inflated_lz4_content_size.puffin"

echo "--- $(basename "$PUFFIN") ---"
$CLICKHOUSE_LOCAL -q "SELECT blob_type FROM file('$PUFFIN', PuffinMetadata)" 2>&1 | grep -oF 'Puffin footer LZ4 content size'

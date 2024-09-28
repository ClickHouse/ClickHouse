#!/usr/bin/env bash
# Tags: long, no-debug, no-parallel
# No parallel: querying `system.tables` with a `Merge` table can throw an exception 
# if a database under the `Merge` table has been dropped at the same time
# if you query columns related to total table size in rows/bytes.

# Test fix for issue #5066

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# NOTE: database = $CLICKHOUSE_DATABASE is unwanted
for _ in {1..100}; do \
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables UNION ALL SELECT name FROM system.columns format Null";
done

#!/usr/bin/env bash
# Tags: long, no-debug

# Test fix for issue #5066

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# NOTE: database = $CLICKHOUSE_DATABASE is unwanted
for _ in {1..100}; do \
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables UNION ALL SELECT name FROM system.columns format Null";
done

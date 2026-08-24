#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Among 1000 invocations, the values are equal most of the time, but let's expect at least 10% of equality to avoid flakiness
# Note: they could be non-equal due to timing differences.

yes "SELECT sum(length(source_part_names)) = (SELECT value FROM system.metrics WHERE name = 'MergeParts') FROM system.merges;" | head -n1000 | $CLICKHOUSE_CLIENT | {
  sort | uniq -cd | awk '$1 > 100 && $NF == "1" { print $NF }'
}

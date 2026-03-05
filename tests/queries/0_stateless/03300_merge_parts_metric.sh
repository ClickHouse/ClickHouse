#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Among 1000 invocations, the values are equal most of the time
# Note: they could be non-equal due to timing differences.

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test; CREATE TABLE test (x Bool) ENGINE = Memory;"
yes "INSERT INTO test SELECT sum(length(source_part_names)) = (SELECT value FROM system.metrics WHERE name = 'MergeParts') FROM system.merges;" | head -n1000 | $CLICKHOUSE_CLIENT
$CLICKHOUSE_CLIENT --query "SELECT sum(x) > 500 FROM test; DROP TABLE test;"

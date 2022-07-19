#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t"

$CLICKHOUSE_CLIENT --query="CREATE TABLE t ENGINE=MergeTree ORDER BY n AS SELECT number AS n FROM numbers(10)"

$CLICKHOUSE_CLIENT --query="ALTER TABLE t ADD INDEX test_index n TYPE minmax GRANULARITY 32"

$CLICKHOUSE_CLIENT --query="ALTER TABLE t DROP INDEX test_indes" 2>&1 | grep -q "may be you meant: \['test_index'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="ALTER TABLE t ADD INDEX  test_index1 n TYPE minmax GRANULARITY 4 AFTER test_indes" 2>&1 | grep -q "may be you meant: \['test_index'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="DROP TABLE t"

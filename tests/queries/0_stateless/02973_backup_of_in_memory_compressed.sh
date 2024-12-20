#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Because we are creating a backup with fixed path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS test;
CREATE TABLE test (x String) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO test SELECT 'Hello, world' FROM numbers(1000000);
"

$CLICKHOUSE_CLIENT --multiquery "
BACKUP TABLE test TO File('test.zip');
" --format Null

$CLICKHOUSE_CLIENT --multiquery "
TRUNCATE TABLE test;
SELECT count() FROM test;
"

$CLICKHOUSE_CLIENT --multiquery "
RESTORE TABLE test FROM File('test.zip');
" --format Null

$CLICKHOUSE_CLIENT --multiquery "
SELECT count(), min(x), max(x) FROM test;
DROP TABLE test;
"

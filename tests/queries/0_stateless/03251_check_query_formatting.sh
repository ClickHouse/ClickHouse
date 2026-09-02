#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --query "CHECK TABLE test PART 'Hello'"
$CLICKHOUSE_FORMAT --query "CHECK TABLE test PARTITION 'Hello'"
$CLICKHOUSE_FORMAT --query "CHECK TABLE test PARTITION tuple()"
$CLICKHOUSE_FORMAT --query "CHECK TABLE test PARTITION ()"
$CLICKHOUSE_FORMAT --query "CHECK TABLE test PARTITION (1, 'Hello', ['World'])"
$CLICKHOUSE_FORMAT --query "CHECK TABLE test PARTITION ALL"

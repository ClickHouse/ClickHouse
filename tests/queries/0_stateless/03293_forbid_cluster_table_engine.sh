#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE test AS s3Cluster('test_shard_localhost', 'http://localhost:11111/test/a.tsv', 'TSV', 'a Int32, b Int32, c Int32');" 2>&1 | grep -cF 'Cannot create a table with *Cluster engine'

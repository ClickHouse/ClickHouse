#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --multiquery "CREATE TABLE test (x UInt8) ENGINE = MergeTree ORDER BY (); INSERT INTO test SELECT 1; SELECT table, name, rows FROM system.parts WHERE database = currentDatabase();"

#!/usr/bin/env bash
# DROP ... SYNC in clickhouse-local with --path returns also when no metadata is loaded from the path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$dir"

# max_execution_time turns a wait that never ends into an error.
queries="
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 1;
DROP TABLE t SYNC;
CREATE VIEW v AS SELECT 1;
DROP VIEW v SYNC;
CREATE DATABASE d;
CREATE TABLE d.t (x UInt8) ENGINE = Memory;
DROP DATABASE d SYNC;
SELECT value FROM system.metrics WHERE metric = 'TablesToDropQueueSize';"

${CLICKHOUSE_LOCAL} --path="$dir/fresh" --only-system-tables --max_execution_time 60 --query "$queries SELECT 'fresh path, only-system-tables'" < /dev/null
${CLICKHOUSE_LOCAL} --path="$dir/existing" --query "CREATE TABLE keep (x UInt8) ENGINE = MergeTree ORDER BY x; INSERT INTO keep VALUES (7)" < /dev/null
${CLICKHOUSE_LOCAL} --path="$dir/existing" --only-system-tables --max_execution_time 60 --query "$queries SELECT 'existing path, only-system-tables'" < /dev/null
${CLICKHOUSE_LOCAL} --path="$dir/existing" --query "SELECT 'keep', sum(x) FROM keep" < /dev/null
${CLICKHOUSE_LOCAL} --path="$dir/plain" --max_execution_time 60 --query "$queries SELECT 'fresh path'" < /dev/null

rm -rf "$dir"

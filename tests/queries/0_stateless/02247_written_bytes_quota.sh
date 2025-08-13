#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS written_bytes_02247"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r02247"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u02247"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q02247"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE written_bytes_02247(s String) ENGINE = Memory"

${CLICKHOUSE_CLIENT} -q "CREATE ROLE r02247"
${CLICKHOUSE_CLIENT} -q "CREATE USER u02247"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON *.* TO r02247"
${CLICKHOUSE_CLIENT} -q "GRANT r02247 to u02247"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q02247 FOR INTERVAL 100 YEAR MAX WRITTEN BYTES = 30 TO r02247"

# The value 'qwqw' means about 13 bytes are to be written, so the current quota (30 bytes) gives the ability to write 'qwqw' 2 times.
${CLICKHOUSE_CLIENT} --user u02247 --async_insert 1 -q "INSERT INTO written_bytes_02247 VALUES ('qwqw')"
#${CLICKHOUSE_CLIENT} --user u02247 -q "SHOW CURRENT QUOTA"
${CLICKHOUSE_CLIENT} --user u02247 --async_insert 0 -q "INSERT INTO written_bytes_02247 VALUES ('qwqw')"
#${CLICKHOUSE_CLIENT} --user u02247 -q "SHOW CURRENT QUOTA"
${CLICKHOUSE_CLIENT} --user u02247 --async_insert 1 -q "INSERT INTO written_bytes_02247 VALUES ('qwqw')" 2>&1 | grep -m1 -o QUOTA_EXCEEDED
${CLICKHOUSE_CLIENT} --user u02247 --async_insert 0 -q "INSERT INTO written_bytes_02247 VALUES ('qwqw')" 2>&1 | grep -m1 -o QUOTA_EXCEEDED

${CLICKHOUSE_CLIENT} -q "SELECT written_bytes > 10 FROM system.quotas_usage WHERE quota_name = 'q02247'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM written_bytes_02247"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA q02247"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA q02247 FOR INTERVAL 100 YEAR MAX WRITTEN BYTES = 1000 TO r02247"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE written_bytes_02247"

# Numbers from 0 to 50 means about 540 bytes are to be written, so the current quota (1000 bytes) is enough to do so.
${CLICKHOUSE_CLIENT} --user u02247 -q "INSERT INTO written_bytes_02247 SELECT toString(number) FROM numbers(50)"

# Numbers from 0 to 100 means about 1090 bytes are to be written, so the current quota (1000 bytes total - 540 bytes already used) is NOT enough to do so.
${CLICKHOUSE_CLIENT} --user u02247 -q "INSERT INTO written_bytes_02247 SELECT toString(number) FROM numbers(100)" 2>&1 | grep -m1 -o QUOTA_EXCEEDED

${CLICKHOUSE_CLIENT} -q "SELECT written_bytes > 100 FROM system.quotas_usage WHERE quota_name = 'q02247'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM written_bytes_02247"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS written_bytes_02247"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS r02247"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u02247"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS q02247"

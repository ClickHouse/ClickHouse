#!/usr/bin/env bash
# Tags: no-fasttest
# `S3Queue` rejects a setting given twice in a new definition (`CREATE` or a full-definition `ATTACH`), but accepts it
# in a stored one, with a warning. The changes are applied in order, so the last value is the one the table uses, and
# the warning says so.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ENGINE="S3Queue('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/*.csv', NOSIGN, 'CSV')"
SETTINGS="mode = 'unordered', keeper_path = '/clickhouse/test_${CLICKHOUSE_DATABASE}/q', polling_min_timeout_ms = 1111, polling_min_timeout_ms = 2222"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE q (a String) ENGINE = ${ENGINE} SETTINGS ${SETTINGS}" 2>&1 \
    | grep -o -m1 'Setting polling_min_timeout_ms is defined multiple times'

# An `Atomic` or `Replicated` database attaches a table with a full definition only with its UUID; `Ordinary` has none.
UUID_CLAUSE=""
if [ "$(${CLICKHOUSE_CLIENT} --query "SELECT engine FROM system.databases WHERE name = currentDatabase()")" != "Ordinary" ]; then
    UUID_CLAUSE="UUID '$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")'"
fi
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE q ${UUID_CLAUSE} (a String) ENGINE = ${ENGINE} SETTINGS ${SETTINGS}" 2>&1 \
    | grep -o -m1 'Setting polling_min_timeout_ms is defined multiple times'

# The stored definition of a table with the setting given once is rewritten to give it twice, and the table is
# attached back with the short `ATTACH`.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE q (a String) ENGINE = ${ENGINE} SETTINGS mode = 'unordered', keeper_path = '/clickhouse/test_${CLICKHOUSE_DATABASE}/q', polling_min_timeout_ms = 1111"
metadata_file=$(${CLICKHOUSE_CLIENT} --query "SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = currentDatabase() AND name = 'q'")
${CLICKHOUSE_CLIENT} --query "DETACH TABLE q"
sed -i.bak 's/polling_min_timeout_ms = 1111/polling_min_timeout_ms = 1111, polling_min_timeout_ms = 2222/' "$metadata_file" && rm -f "$metadata_file.bak"
${CLICKHOUSE_CLIENT} --send_logs_level=warning --query "ATTACH TABLE q" 2>&1 \
    | grep -o -m1 "has a duplicated setting polling_min_timeout_ms. Will use the [a-z]* declared setting's value"

${CLICKHOUSE_CLIENT} --query "SELECT name, value FROM system.s3_queue_settings WHERE database = currentDatabase() AND table = 'q' AND name = 'polling_min_timeout_ms'"

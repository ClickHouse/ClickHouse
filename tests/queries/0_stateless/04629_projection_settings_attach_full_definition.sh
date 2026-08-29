#!/usr/bin/env bash
# A full-definition `ATTACH TABLE` is CREATE-like user input, so a projection `WITH SETTINGS`
# clause in it must pass the same allow-list and sanity checks as `CREATE TABLE`, instead of
# being treated as a metadata load (which sanitizes silently and skips the allow-list).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate a random UUID to avoid collisions in Atomic databases.
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# A projection setting outside the allow-list is rejected on a full-definition ATTACH,
# the same way CREATE TABLE rejects it.
# -m1 because the error message may contain the error code name multiple times.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_proj_attach_full UUID '${UUID}' (x UInt64, PROJECTION p (SELECT x ORDER BY x) WITH SETTINGS (marks_compression_codec = 'LZ4')) ENGINE = MergeTree ORDER BY x;" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

# A projection setting from the allow-list works on a full-definition ATTACH.
# `send_logs_level=fatal` suppresses the "full table definition is not recommended" warning.
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level fatal -q "ATTACH TABLE t_proj_attach_full UUID '${UUID}' (x UInt64, PROJECTION p (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 555)) ENGINE = MergeTree ORDER BY x;"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_proj_attach_full SELECT number FROM numbers(1000);"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_proj_attach_full;"

# The short ATTACH (stored metadata) still works.
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_proj_attach_full;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_proj_attach_full;"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_proj_attach_full;"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_proj_attach_full;"

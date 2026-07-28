#!/usr/bin/env bash
# A full-definition `ATTACH TABLE` is CREATE-like user input, so an experimental codec
# in the codec-valued MergeTree settings must require `allow_experimental_codecs` there
# as well, unlike a short `ATTACH TABLE t` that reads the definition from stored metadata.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate a random UUID to avoid collisions in Atomic databases.
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# The gate applies to a full-definition ATTACH without the setting.
# -m1 because the error message may contain the error code name multiple times.
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "ATTACH TABLE t_zxc_attach_full UUID '${UUID}' (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'ZXC';" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "ATTACH TABLE t_zxc_attach_full UUID '${UUID}' (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS marks_compression_codec = 'ZXC';" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "ATTACH TABLE t_zxc_attach_full UUID '${UUID}' (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS primary_key_compression_codec = 'ZXC';" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

# With the setting enabled, the same full-definition ATTACH works.
# `send_logs_level=fatal` suppresses the "full table definition is not recommended" warning.
$CLICKHOUSE_CLIENT --allow_experimental_codecs 1 --send_logs_level fatal -q "ATTACH TABLE t_zxc_attach_full UUID '${UUID}' (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'ZXC';"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_zxc_attach_full SELECT number FROM numbers(1000);"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_zxc_attach_full;"

# The short ATTACH (stored metadata) still works without the setting.
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "DETACH TABLE t_zxc_attach_full;"
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "ATTACH TABLE t_zxc_attach_full;"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_zxc_attach_full;"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_zxc_attach_full;"

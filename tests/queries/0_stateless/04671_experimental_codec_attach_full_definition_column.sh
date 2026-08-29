#!/usr/bin/env bash
# A full-definition `ATTACH TABLE t (...)` is CREATE-like user input, so an experimental codec in a
# column `CODEC(...)` clause must require `allow_experimental_codecs` there as well, unlike a short
# `ATTACH TABLE t` that reads the definition back from metadata stored on this server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate a random UUID to avoid collisions in Atomic databases.
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# The experimental gate applies to a full-definition ATTACH without the setting, both for the codec
# alone and inside a chain.
# -m1 because the error message may contain the error code name multiple times.
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "ATTACH TABLE t_zxc_attach_column UUID '${UUID}' (x UInt64 CODEC(ZXC)) ENGINE = MergeTree ORDER BY tuple();" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "ATTACH TABLE t_zxc_attach_column UUID '${UUID}' (x UInt64 CODEC(Delta, ZXC)) ENGINE = MergeTree ORDER BY tuple();" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

# The suspicious-codec sanity checks apply to a full-definition ATTACH too.
$CLICKHOUSE_CLIENT --allow_suspicious_codecs 0 -q "ATTACH TABLE t_zxc_attach_column UUID '${UUID}' (x UInt64 CODEC(ZSTD, LZ4)) ENGINE = MergeTree ORDER BY tuple();" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

# With the setting enabled, the same full-definition ATTACH works.
# `send_logs_level=fatal` suppresses the "full table definition is not recommended" warning.
$CLICKHOUSE_CLIENT --allow_repeated_settings --allow_experimental_codecs 1 --send_logs_level fatal -q "ATTACH TABLE t_zxc_attach_column UUID '${UUID}' (x UInt64 CODEC(ZXC)) ENGINE = MergeTree ORDER BY tuple();"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_zxc_attach_column SELECT number FROM numbers(1000);"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_zxc_attach_column;"

# The short ATTACH (stored metadata) still works without the setting.
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "DETACH TABLE t_zxc_attach_column;"
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "ATTACH TABLE t_zxc_attach_column;"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_zxc_attach_column;"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_zxc_attach_column;"

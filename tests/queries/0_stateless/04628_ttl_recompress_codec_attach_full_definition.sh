#!/usr/bin/env bash
# A full-definition `ATTACH TABLE` is CREATE-like user input, so an experimental codec in a
# `TTL ... RECOMPRESS` clause must require `allow_experimental_codecs` there as well, unlike a
# short `ATTACH TABLE t` that reads the definition from stored metadata. A codec that requires
# a column type (e.g. `T64`) is rejected outright, exactly as at `CREATE`, instead of being
# silently normalized like on a genuine metadata load.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate a random UUID to avoid collisions in Atomic databases.
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# The experimental-codec gate applies to a full-definition ATTACH without the setting.
# -m1 because the error message may contain the error code name multiple times.
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "ATTACH TABLE t_zxc_ttl_attach_full UUID '${UUID}' (d Date, x UInt64) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZXC);" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

# A codec that requires a column type is rejected regardless of the setting, exactly as at CREATE.
$CLICKHOUSE_CLIENT --allow_experimental_codecs 1 -q "ATTACH TABLE t_zxc_ttl_attach_full UUID '${UUID}' (d Date, x UInt64) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(T64);" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

# With the setting enabled, the same full-definition ATTACH works.
# `send_logs_level=fatal` suppresses the "full table definition is not recommended" warning.
$CLICKHOUSE_CLIENT --allow_repeated_settings --allow_experimental_codecs 1 --send_logs_level fatal -q "ATTACH TABLE t_zxc_ttl_attach_full UUID '${UUID}' (d Date, x UInt64) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZXC);"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_zxc_ttl_attach_full SELECT today() + 10, number FROM numbers(1000);"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_zxc_ttl_attach_full;"

# The short ATTACH (stored metadata) still works without the setting.
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "DETACH TABLE t_zxc_ttl_attach_full;"
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "ATTACH TABLE t_zxc_ttl_attach_full;"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_zxc_ttl_attach_full;"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_zxc_ttl_attach_full;"

# `allow_suspicious_ttl_expressions` relaxes the TTL *expression* checks only: it is not a codec
# escape hatch, so it does not admit an experimental codec on a full-definition ATTACH (nor at CREATE).
UUID2=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 --allow_suspicious_ttl_expressions 1 -q "ATTACH TABLE t_zxc_ttl_attach_hatch UUID '${UUID2}' (d Date, x UInt64) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(ZXC);" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

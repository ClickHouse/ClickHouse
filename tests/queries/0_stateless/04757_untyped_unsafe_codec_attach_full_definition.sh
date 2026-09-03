#!/usr/bin/env bash
# A full-definition `ATTACH TABLE` is CREATE-like user input, so a codec that requires
# the column type (e.g. `T64`) in the codec-valued MergeTree settings must be rejected
# there regardless of `allow_experimental_codecs` — the same way CREATE rejects it —
# instead of being silently dropped by the load-path sanitization.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate a random UUID to avoid collisions in Atomic databases.
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# `T64` is not experimental, but it cannot compress an untyped stream; enabling
# `allow_experimental_codecs` must not bypass this part of the validation.
# -m1 because the error message may contain the error code name multiple times.
$CLICKHOUSE_CLIENT --allow_experimental_codecs 1 -q "ATTACH TABLE t_t64_attach_full UUID '${UUID}' (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'T64';" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --allow_experimental_codecs 1 -q "ATTACH TABLE t_t64_attach_full UUID '${UUID}' (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS marks_compression_codec = 'T64';" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --allow_experimental_codecs 1 -q "ATTACH TABLE t_t64_attach_full UUID '${UUID}' (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS primary_key_compression_codec = 'T64';" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT --allow_experimental_codecs 0 -q "ATTACH TABLE t_t64_attach_full UUID '${UUID}' (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'T64';" 2>&1 | grep -m 1 -o -F 'BAD_ARGUMENTS'

# A safe codec in the same position still works.
# `send_logs_level=fatal` suppresses the "full table definition is not recommended" warning.
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level fatal -q "ATTACH TABLE t_t64_attach_full UUID '${UUID}' (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS default_compression_codec = 'ZSTD(3)';"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_t64_attach_full SELECT number FROM numbers(1000);"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_t64_attach_full;"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_t64_attach_full;"

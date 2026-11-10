#!/usr/bin/env bash
# A full-definition `ATTACH TABLE` is CREATE-like user input, so naming the internal database
# of temporary tables in the `Merge` engine must be denied there as well, the same way as in
# `CREATE TABLE`; otherwise the forbidden and unusable definition would be persisted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate a random UUID to avoid collisions in Atomic databases.
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# -m1 because the error message may contain the error code name multiple times.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_merge_tmp_attach_full UUID '${UUID}' (dummy UInt8) ENGINE = Merge('_temporary_and_external_tables', '^_tmp_');" 2>&1 | grep -m 1 -o -F 'DATABASE_ACCESS_DENIED'

# A short ATTACH (stored metadata) of a legitimate definition still works.
# `send_logs_level=fatal` suppresses the "full table definition is not recommended" warning.
$CLICKHOUSE_CLIENT --send_logs_level fatal -q "ATTACH TABLE t_merge_tmp_attach_full UUID '${UUID}' (dummy UInt8) ENGINE = Merge('system', '^one$');"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_merge_tmp_attach_full;"
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_merge_tmp_attach_full;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_merge_tmp_attach_full;"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_merge_tmp_attach_full;"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_merge_tmp_attach_full;"

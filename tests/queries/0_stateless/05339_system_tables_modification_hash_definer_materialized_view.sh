#!/usr/bin/env bash

# The `modification_hash` of a `SQL SECURITY DEFINER` materialized view covers its whole target table,
# which is read with the definer's grants. In `system.tables` it must be `NULL` for a caller who can
# read the view but not every column of the target: otherwise polling it would reveal changes to the
# target-only columns the caller cannot read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# Users are server-wide, so make the names unique per run.
user="user_05339_${CLICKHOUSE_DATABASE}"
definer="definer_05339_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_05339 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE target_05339 (x UInt64, secret UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE USER ${definer};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src_05339 TO ${definer};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.target_05339 TO ${definer};
    CREATE MATERIALIZED VIEW mv_05339 TO target_05339 (x UInt64)
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src_05339;
    INSERT INTO src_05339 VALUES (1);
    CREATE USER ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.mv_05339 TO ${user};
"

function show_hash()
{
    $CLICKHOUSE_CLIENT --user "${user}" -q "
        SELECT '$1', isNotNull(modification_hash) FROM system.tables
        WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'mv_05339'"
}

show_hash 'view only'

$CLICKHOUSE_CLIENT -q "GRANT SELECT(x) ON ${CLICKHOUSE_DATABASE}.target_05339 TO ${user}"
show_hash 'exposed target column'

$CLICKHOUSE_CLIENT -q "GRANT SELECT(secret) ON ${CLICKHOUSE_DATABASE}.target_05339 TO ${user}"
show_hash 'every target column'

$CLICKHOUSE_CLIENT -q "DROP TABLE mv_05339; DROP USER ${user}, ${definer}"

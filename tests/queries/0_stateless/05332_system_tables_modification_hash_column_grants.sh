#!/usr/bin/env bash

# `system.tables.modification_hash` covers the whole table, so it must be `NULL` for a caller who can
# read only some of its columns: otherwise polling it would reveal changes to the columns the caller
# cannot read. The same applies to a table reached through a wrapper engine such as `Merge`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# Users are server-wide, so make the name unique per run.
user="user_05332_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_05332 (public UInt64, secret UInt64) ENGINE = MergeTree ORDER BY public;
    INSERT INTO t_05332 VALUES (1, 1);
    CREATE TABLE m_05332 (public UInt64, secret UInt64) ENGINE = Merge(currentDatabase(), '^t_05332\$');
    CREATE USER ${user};
    GRANT SELECT(public) ON ${CLICKHOUSE_DATABASE}.t_05332 TO ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.m_05332 TO ${user};
"

function show_hash()
{
    $CLICKHOUSE_CLIENT --user "${user}" -q "
        SELECT '$1', name, isNotNull(modification_hash) FROM system.tables
        WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN ('t_05332', 'm_05332') ORDER BY name"
}

show_hash 'one column'

$CLICKHOUSE_CLIENT -q "GRANT SELECT(secret) ON ${CLICKHOUSE_DATABASE}.t_05332 TO ${user}"
show_hash 'every column'

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"

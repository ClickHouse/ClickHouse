#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# An invalid value of exclude_materialize_statistics_on_insert or exclude_materialize_statistics_on_merge is
# rejected with an error that names the setting, wherever it is checked.
#
# A full-definition ATTACH states its settings itself, so it checks exclude_materialize_statistics_on_merge the
# way CREATE does. An `Atomic` database only takes such a definition together with the UUID of the table.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_exclude_stats (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS auto_statistics_types = 'basic', exclude_materialize_statistics_on_merge = 'a b';
" 2>&1 | grep -om1 "Cannot parse the value 'a b' of setting 'exclude_materialize_statistics_on_merge' as a comma-separated list of column names: unexpected 'b' at position 3"

$CLICKHOUSE_CLIENT -q "
    ATTACH TABLE t_exclude_stats UUID '$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')'
    (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS auto_statistics_types = 'basic', exclude_materialize_statistics_on_merge = 'n.1';
" 2>&1 | grep -om1 "Cannot parse the value 'n.1' of setting 'exclude_materialize_statistics_on_merge' as a comma-separated list of column names: unexpected '1' at position 3"

# A valid value is attached. The server warns about a full definition, so its log is left out.
$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    ATTACH TABLE t_exclude_stats UUID '$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')'
    (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS auto_statistics_types = 'basic', exclude_materialize_statistics_on_merge = 'b';
    SELECT extract(engine_full, 'exclude_materialize_statistics_on_merge = [^,]+')
    FROM system.tables WHERE database = currentDatabase() AND name = 't_exclude_stats';
"

$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_exclude_stats MODIFY SETTING exclude_materialize_statistics_on_merge = '\'\'';
" 2>&1 | grep -om1 "Cannot parse the value '\\\\'\\\\'' of setting 'exclude_materialize_statistics_on_merge' as a comma-separated list of column names: a column name cannot be empty"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_exclude_stats SETTINGS materialize_statistics_on_insert = 1, exclude_materialize_statistics_on_insert = 'a,' VALUES (1, 1);
" 2>&1 | grep -om1 "Cannot parse the value 'a,' of setting 'exclude_materialize_statistics_on_insert' as a comma-separated list of column names: unexpected end of string at position 3"

$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_exclude_stats;
    DROP TABLE t_exclude_stats;
"

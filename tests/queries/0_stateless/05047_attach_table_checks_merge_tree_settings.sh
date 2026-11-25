#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A full-definition ATTACH states its settings itself, so they are checked the way CREATE checks them.
# An `Atomic` database only takes such a definition together with the UUID of the table.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_attach_checks (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 0;
" 2>&1 | grep -om1 "index_granularity: value 0 makes no sense"

$CLICKHOUSE_CLIENT -q "
    ATTACH TABLE t_attach_checks UUID '$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')'
    (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 0;
" 2>&1 | grep -om1 "index_granularity: value 0 makes no sense"

# A value that passes is still attached. The server warns about a full definition, which is what this
# test is about, so its log is left out.
$CLICKHOUSE_CLIENT --send_logs_level=error -q "
    ATTACH TABLE t_attach_checks UUID '$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')'
    (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024;
    SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_attach_checks';
    DROP TABLE t_attach_checks;
"

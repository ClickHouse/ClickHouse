#!/usr/bin/env bash
# A `database` setting naming a database that does not exist must be reported, not wedge the thread
# that runs the query.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The assertion is the error code. `timeout` is only a harness so that a regression fails here
# instead of hanging the suite.
timeout 60 $CLICKHOUSE_CLIENT --query "SELECT 1 SETTINGS database = 'no_such_db_$CLICKHOUSE_DATABASE'" 2>&1 \
    | grep -c -m1 UNKNOWN_DATABASE

# An existing database is still selected. `system` is used rather than the current database because
# the setting is only applied when it names a different database, and resolving `tables` unqualified
# shows that the switch took effect.
$CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM tables SETTINGS database = 'system'"

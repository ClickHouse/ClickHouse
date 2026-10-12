#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Both RESTRICTIVE and PERMISSIVE should be formatted in uppercase (issue #105352)
echo "CREATE ROW POLICY p ON db.t AS restrictive FOR SELECT USING 1 TO r1" | $CLICKHOUSE_FORMAT
echo "CREATE ROW POLICY p ON db.t AS permissive FOR SELECT USING 1 TO r1" | $CLICKHOUSE_FORMAT
echo "CREATE ROW POLICY p ON db.t AS RESTRICTIVE FOR SELECT USING 1 TO r1" | $CLICKHOUSE_FORMAT
echo "CREATE ROW POLICY p ON db.t AS PERMISSIVE FOR SELECT USING 1 TO r1" | $CLICKHOUSE_FORMAT

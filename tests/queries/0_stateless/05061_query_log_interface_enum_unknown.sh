#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test
python3 "$CUR_DIR"/05061_query_log_interface_enum_unknown.python

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# Each row also carries an in-domain value of the other column, so an always-`Unknown` projection
# would not pass: `interface` is `HTTP` in the second row, `http_method` is `UNKNOWN` in the first.
$CLICKHOUSE_CLIENT -q "
    SELECT log_comment, interface, http_method
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('interface_enum_unknown', 'http_method_enum_unknown')
      AND type = 'QueryStart'
    ORDER BY log_comment
"

# Rendering every column of those rows must not throw either.
$CLICKHOUSE_CLIENT -q "
    SELECT *
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('interface_enum_unknown', 'http_method_enum_unknown')
    FORMAT Vertical
" > /dev/null && echo 'all columns render'

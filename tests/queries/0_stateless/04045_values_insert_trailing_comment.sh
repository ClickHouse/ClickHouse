#!/usr/bin/env bash
# Tags: no-fasttest

# A trailing SQL comment after the VALUES data of an INSERT inside a multi-query
# stream must not swallow the following queries. This regresses issue #109253:
# under the server-side inline path (send_table_structure_on_insert_with_inline_data=0)
# the multi-query boundary guess scanned the comment as row data past the terminating
# ';' to end of input, silently dropping subsequent queries. Exercise both the legacy
# (=1) and inline (=0) client paths explicitly so the fix is covered regardless of
# CI randomization.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for mode in 1 0; do
    echo "--- send_table_structure_on_insert_with_inline_data=${mode} ---"
    $CLICKHOUSE_CLIENT --send_table_structure_on_insert_with_inline_data="$mode" -n --query "
CREATE OR REPLACE TABLE test_values_trailing_comment (a String, b Int32, c Int32, d Int32) ENGINE = MergeTree() ORDER BY a;
INSERT INTO test_values_trailing_comment SETTINGS async_insert=0 VALUES
('123456', 1, 10, 100),
('123457', 2, 20, 100)
-- trailing comment after last row
;
SELECT * FROM test_values_trailing_comment ORDER BY a;
DROP TABLE test_values_trailing_comment;
"
done

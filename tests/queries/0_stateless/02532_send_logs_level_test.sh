#!/usr/bin/env bash
# Tags: no-object-storage, no-debug, no-random-merge-tree-settings
# - no-object-storage - S3 has additional logging
# - no-debug - debug builds also has additional logging
# - no-random-merge-tree-settings - changes content of log messages

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists data;
    create table data (key Int) engine=MergeTree order by tuple() settings min_bytes_for_wide_part = '1G', compress_marks = 1;
    insert into data values (1);
"

# NOTE: boost multitoken (--allow_repeated_settings) could prefer "first"
# instead of "last" value, hence you cannot simply append another
# --send_logs_level here.
CLICKHOUSE_CLIENT_CLEAN=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=test/g')

set -e

trap '$CLICKHOUSE_CLIENT -q "drop table data"' EXIT

$CLICKHOUSE_CLIENT_CLEAN -q "select * from data SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;" |& (! grep -q -o -e '<Unknown>.*')
$CLICKHOUSE_CLIENT_CLEAN -q "select * from data SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;" |& grep -q -o -e '<Test>.*'

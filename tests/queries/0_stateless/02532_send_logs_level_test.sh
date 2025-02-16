#!/usr/bin/env bash
# Tags: no-s3-storage, no-debug
# - no-s3-storage - S3 has additional logging
# - no-debug - debug builds also has additional logging

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data;
    create table data (key Int) engine=MergeTree order by tuple();
    insert into data values (1);
"

# NOTE: boost multitoken (--allow_repeated_settings) could prefer "first"
# instead of "last" value, hence you cannot simply append another
# --send_logs_level here.
CLICKHOUSE_CLIENT_CLEAN=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=test/g')
$CLICKHOUSE_CLIENT_CLEAN -q "select * from data SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;" |& grep -o -e '<Unknown>.*' -e '<Test>.*'

$CLICKHOUSE_CLIENT -q "drop table data"

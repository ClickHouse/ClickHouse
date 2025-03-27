#!/usr/bin/env bash
set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} \
   $CLICKHOUSE_URL \
   --get \
   --data-urlencode "query=
     select number
     from numbers_mt(1e6)
     where number = 42
     group by number
     settings max_threads = 10, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1, output_format_pretty_color=1
     format PrettyCompact"

${CLICKHOUSE_CURL} \
  $CLICKHOUSE_URL \
  --get \
  --data-urlencode "query=
    select number
    from numbers_mt(1e6)
    where number = 42
    group by number
    settings max_threads = 10, max_bytes_before_external_group_by = 0, group_by_two_level_threshold = 1, output_format_pretty_color=1
    format PrettyCompact"

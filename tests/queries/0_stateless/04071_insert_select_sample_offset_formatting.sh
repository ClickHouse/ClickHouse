#!/usr/bin/env bash
# BuzzHouse: INSERT with SAMPLE and OFFSET should not use FROM-first syntax

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ch_format() {
    $CLICKHOUSE_FORMAT --oneline
}

format_query() {
  local query="$1"
  echo "Original:          $query"
  local formatted="$(echo "$query" | ch_format)"
  echo "format(original):  $formatted"
  echo 'format(formatted):' "$(echo "$formatted" | ch_format)"
  echo
}

# SAMPLE + standalone OFFSET (no LIMIT) + WHERE triggers FROM-first syntax
format_query "INSERT INTO dest SELECT * FROM src SAMPLE 0.1 WHERE x > 0 OFFSET 3"

# Nested subquery with WHERE: inner SELECT should not be affected by disable_from_first_syntax
format_query "INSERT INTO t SELECT * FROM (SELECT x FROM src SAMPLE 0.1 WHERE x > 0 OFFSET 3)"

# Nested subquery without WHERE: ensure SAMPLE + standalone OFFSET remains reparsable and idempotent
format_query "INSERT INTO t SELECT * FROM (SELECT x FROM src SAMPLE 0.1 OFFSET 3)"

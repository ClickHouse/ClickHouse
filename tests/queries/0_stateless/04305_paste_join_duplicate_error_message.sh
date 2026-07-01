#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Duplicate PASTE JOIN columns must report a well-formed message with a space before "While processing".
# https://github.com/ClickHouse/ClickHouse/issues/104434
$CLICKHOUSE_CLIENT --joined_subquery_requires_alias 1 -q \
    "SELECT * FROM (SELECT 1 AS a) PASTE JOIN (SELECT 1 AS a)" 2>&1 \
    | grep -oF 'avoid duplication) While processing'

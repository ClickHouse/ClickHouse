#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We should not print all subsequent queries on a syntax error inside a single query:
yes "EXPLAIN TABLE OVERRIDE SELECT 1;" | head -n10 | ${CLICKHOUSE_LOCAL} --ignore-error 2>&1 | grep -F 'EXPLAIN TABLE OVERRIDE' | wc -l

#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

run()
{
    echo "--- $1"
    ${CLICKHOUSE_CLIENT} -q "$1" 2>&1 | grep -m1 -E '^Code: ' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: Received from [^ ]* DB::Exception: /Code: \1. /' \
              -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' \
              -e 's/: While processing .*//' -e 's/ (version [^)]*)$//'
}

run "SELECT 1 UNION SELECT 2"
run "SELECT 1 EXCEPT SELECT 2 SETTINGS except_default_mode = ''"
run "SELECT 1 INTERSECT SELECT 2 SETTINGS intersect_default_mode = ''"

# The modes that are spelled out, and the ones a default is configured for, still work.
${CLICKHOUSE_CLIENT} -q "SELECT 1 UNION ALL SELECT 1 ORDER BY 1"
${CLICKHOUSE_CLIENT} -q "SELECT 1 UNION DISTINCT SELECT 1"
${CLICKHOUSE_CLIENT} -q "SELECT 1 UNION SELECT 1 SETTINGS union_default_mode = 'DISTINCT'"

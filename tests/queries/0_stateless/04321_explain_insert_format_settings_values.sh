#!/usr/bin/env bash
# Follow-up to https://github.com/ClickHouse/ClickHouse/pull/101772 (#67321):
# FORMAT must apply to the EXPLAIN output of an INSERT ... SELECT even when SETTINGS precede
# the FORMAT or the output format is named Values, while SETTINGS that follow the FORMAT
# (allow_settings_after_format_in_insert) and input-data formats must stay on the INSERT.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SETTINGS written before the FORMAT belong to the INSERT, but the trailing FORMAT still
# applies to the EXPLAIN output. JSONEachRow wraps the output in JSON.
echo "--- SETTINGS before FORMAT ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 SETTINGS max_threads = 1 FORMAT JSONEachRow" | head -1 | grep -c '^{"explain"'

# Values is a valid EXPLAIN output format; it must not be treated as the INSERT VALUES data path.
echo "--- FORMAT Values ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT Values" | grep -c "^('INSERT"

# SETTINGS after FORMAT (allow_settings_after_format_in_insert) cannot be cleanly rewound, so the
# FORMAT stays on the INSERT and is shown verbatim in the EXPLAIN output.
echo "--- SETTINGS after FORMAT ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT JSONEachRow SETTINGS max_threads = 1" --allow_settings_after_format_in_insert 1 | grep -c 'FORMAT JSONEachRow'

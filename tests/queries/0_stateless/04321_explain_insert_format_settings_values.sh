#!/usr/bin/env bash
# Follow-up to https://github.com/ClickHouse/ClickHouse/pull/101772 (#67321):
# FORMAT must apply to the EXPLAIN output of an INSERT ... SELECT regardless of where SETTINGS go
# and regardless of the output format name (including Values). Only input-data formats (FROM INFILE,
# the `input` table function) and the explicit double-FORMAT form keep the FORMAT on the INSERT.
# The decision must not depend on the clause order, otherwise a formatting roundtrip (the formatter
# always emits SETTINGS before the SELECT and the INSERT FORMAT last) would flip it and trip the
# "Inconsistent AST formatting" assertion. See https://github.com/ClickHouse/ClickHouse/pull/106686

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

# SETTINGS after FORMAT (allow_settings_after_format_in_insert) must behave the same as SETTINGS
# before FORMAT: the FORMAT applies to the EXPLAIN output (JSON), the SETTINGS stay on the INSERT.
echo "--- SETTINGS after FORMAT ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT JSONEachRow SETTINGS max_threads = 1" --allow_settings_after_format_in_insert 1 | head -1 | grep -c '^{"explain"'

# Explicit double-FORMAT: the first FORMAT genuinely belongs to the INSERT and stays on it, while
# the second FORMAT applies to the EXPLAIN output (JSON).
echo "--- double FORMAT ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT Values FORMAT JSONEachRow" | head -1 | grep -c '^{"explain"'

# A trailing ';' query terminator must be accepted: an INSERT ... SELECT (without the `input` table
# function) has no inline data, so the FORMAT applies to the EXPLAIN output and the ';' is not data.
echo "--- trailing semicolon ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT JSONEachRow;" | head -1 | grep -c '^{"explain"'

# The choice of where the FORMAT lands must be stable across a formatting roundtrip, so that the
# AST does not change when re-parsed. Compare the formatted output of the query with the formatted
# output of its own formatted output for every shape above.
echo "--- roundtrip stability ---"
for q in \
    "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 SETTINGS max_threads = 1 FORMAT JSONEachRow" \
    "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT Values" \
    "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT Values FORMAT JSONEachRow"
do
    once=$(echo "$q" | ${CLICKHOUSE_FORMAT})
    twice=$(echo "$once" | ${CLICKHOUSE_FORMAT})
    [ "$once" = "$twice" ] && echo "stable" || echo "UNSTABLE"
done

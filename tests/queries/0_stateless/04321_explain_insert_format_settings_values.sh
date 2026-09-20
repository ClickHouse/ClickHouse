#!/usr/bin/env bash
# Follow-up to https://github.com/ClickHouse/ClickHouse/pull/101772 (#67321):
# FORMAT must apply to the EXPLAIN output of an INSERT ... SELECT regardless of where SETTINGS go
# and regardless of the output format name (including Values). Only input-data formats (FROM INFILE,
# the `input` table function) and the explicit double-FORMAT form keep the FORMAT on the INSERT.
# The decision must not depend on the clause order, otherwise a formatting roundtrip (the formatter
# always emits SETTINGS before the SELECT and the INSERT FORMAT last) would flip it and trip the
# "Inconsistent AST formatting" assertion. See https://github.com/ClickHouse/ClickHouse/pull/106686
#
# The full EXPLAIN output is printed, so the reference checks both that the requested format is
# used for the EXPLAIN output and that the FORMAT clause is gone from (or kept on) the INSERT body.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SETTINGS written before the FORMAT belong to the INSERT, but the trailing FORMAT still
# applies to the EXPLAIN output: the output is JSON and the INSERT body has no FORMAT.
echo "--- SETTINGS before FORMAT ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 SETTINGS max_threads = 1 FORMAT JSONEachRow"

# Values is a valid EXPLAIN output format; it must not be treated as the INSERT VALUES data path:
# the output is Values and the INSERT body has no FORMAT.
echo "--- FORMAT Values ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT Values"
echo "" # The Values format does not end with a newline.

# SETTINGS after FORMAT (allow_settings_after_format_in_insert) must behave the same as SETTINGS
# before FORMAT: the output is JSON, and both the SETTINGS and no FORMAT are on the INSERT body.
echo "--- SETTINGS after FORMAT ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT JSONEachRow SETTINGS max_threads = 1" --allow_settings_after_format_in_insert 1

# Explicit double-FORMAT: the first FORMAT genuinely belongs to the INSERT and stays on its body,
# while the second FORMAT applies to the EXPLAIN output (JSON).
echo "--- double FORMAT ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT Values FORMAT JSONEachRow"

# A trailing ';' query terminator must be accepted: an INSERT ... SELECT (without the `input` table
# function) has no inline data, so the FORMAT applies to the EXPLAIN output and the ';' is not data.
echo "--- trailing semicolon ---"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT JSONEachRow;"

# When INTO OUTFILE follows the moved FORMAT, the FORMAT child is attached to the EXPLAIN query
# before INTO OUTFILE is parsed, so the output-option children must be canonicalized; otherwise the
# AST children order differs from a re-parse of the formatted query (which emits INTO OUTFILE before
# FORMAT) and trips the debug-only "Inconsistent AST formatting" tree-hash check during execution.
echo "--- INTO OUTFILE after moved FORMAT ---"
OUTFILE="${CLICKHOUSE_TMP}/04321_explain_insert_format_settings_values.out"
rm -f "$OUTFILE"
${CLICKHOUSE_CLIENT} --query "EXPLAIN SYNTAX INSERT INTO FUNCTION null('x UInt64') SELECT 1 FORMAT JSONEachRow INTO OUTFILE '$OUTFILE'" && echo "ok"
rm -f "$OUTFILE"

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

#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema sets the typing settings that a constraint's scalar subquery, which CREATE runs, analyzes with, and warns
# when it cannot probe that subquery without running another one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

DB="${CLICKHOUSE_DATABASE}"

# The source server runs the subquery at CREATE, so the table is created there, where its stored CREATE loads.
$CLICKHOUSE_CLIENT -n -q "
    SET allow_lossy_numeric_supertype = 1;
    CREATE TABLE ${DB}.t (id UInt64, CONSTRAINT c CHECK id <= (SELECT arraySum([toDecimal64(1, 2), 0.5]))) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT --dump-schema "$DB" > "$work/probed.sql" 2>"$work/err"
echo "probed: dump rc=$?"
grep '^SET allow_lossy_numeric_supertype' "$work/probed.sql"
echo "probed: unverified warnings: $(grep -c 'could not be verified' "$work/err")"
$CLICKHOUSE_LOCAL --path "$work/dst" --allow_lossy_numeric_supertype=0 -n --queries-file "$work/probed.sql" 2>&1 \
    | grep -o 'Code: [0-9]*' | head -1
echo "probed: replay rc=${PIPESTATUS[0]}"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t"

# The probe of a subquery that holds another scalar subquery would run that one, so the typing stays unverified.
$CLICKHOUSE_CLIENT -n -q "
    SET allow_lossy_numeric_supertype = 1;
    CREATE TABLE ${DB}.t_nested (id UInt64, CONSTRAINT c CHECK id <= (SELECT arraySum([toDecimal64(1, 2), assumeNotNull((SELECT 0.5))]))) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT --dump-schema "$DB" > /dev/null 2>"$work/err"
echo "nested: dump rc=$?"
echo "nested: unverified warnings: $(grep -c "typing settings of ${DB}.t_nested could not be verified" "$work/err")"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t_nested"

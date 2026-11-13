#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: the dump of a `Replicated` database names macros that the replaying `clickhouse-local` does not have.

# --dump-schema sets the typing settings that a constraint's scalar subquery, which CREATE and every ATTACH run, analyzes
# with, and warns when it cannot probe that subquery without running another one or the view it reads.

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
echo "probed: profile warnings: $(grep -c "${DB}.t also need allow_lossy_numeric_supertype = 1 in the replaying server's settings profile" "$work/err")"
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
echo "nested: unverified warnings: $(grep -c "typing settings of the constraint subqueries of ${DB}.t_nested could not be verified" "$work/err")"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t_nested"

# A constraint subquery that cannot be probed leaves the proven typing of a DEFAULT as it is.
$CLICKHOUSE_CLIENT -n -q "
    SET allow_lossy_numeric_supertype = 1;
    CREATE TABLE ${DB}.t_default (id UInt64, v Float64 DEFAULT arraySum([toDecimal64(1, 2), 0.5]), CONSTRAINT c CHECK id <= (SELECT if(1, 1, (SELECT 2)))) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT --dump-schema "$DB" > "$work/default.sql" 2>"$work/err"
echo "default: dump rc=$?"
grep '^SET allow_lossy_numeric_supertype' "$work/default.sql"
echo "default: table unverified warnings: $(grep -c "typing settings of ${DB}.t_default could not be verified" "$work/err")"
echo "default: constraint unverified warnings: $(grep -c "typing settings of the constraint subqueries of ${DB}.t_default could not be verified" "$work/err")"
rm -rf "$work/dst"
$CLICKHOUSE_LOCAL --path "$work/dst" --allow_lossy_numeric_supertype=0 -n --queries-file "$work/default.sql" 2>&1 \
    | grep -o 'Code: [0-9]*' | head -1
echo "default: replay rc=${PIPESTATUS[0]}"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t_default"

# The probes do not inline a view, so the typing of a view that a constraint subquery reads stays unverified.
$CLICKHOUSE_CLIENT -n -q "
    SET allow_lossy_numeric_supertype = 1;
    CREATE VIEW ${DB}.v AS SELECT arraySum([toDecimal64(1, 2), 0.5]) AS s;
    CREATE TABLE ${DB}.t_view (id UInt64, CONSTRAINT c CHECK id <= (SELECT s FROM ${DB}.v)) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT --allow_lossy_numeric_supertype=1 --dump-schema "$DB" > "$work/view.sql" 2>"$work/err"
echo "view: dump rc=$?"
grep '^SET allow_lossy_numeric_supertype' "$work/view.sql"
echo "view: constraint unverified warnings: $(grep -c "typing settings of the constraint subqueries of ${DB}.t_view could not be verified" "$work/err")"
rm -rf "$work/dst"
$CLICKHOUSE_LOCAL --path "$work/dst" --allow_lossy_numeric_supertype=0 -n --queries-file "$work/view.sql" 2>&1 \
    | grep -o 'Code: [0-9]*' | head -1
echo "view: replay rc=${PIPESTATUS[0]}"
$CLICKHOUSE_CLIENT -n -q "DROP TABLE ${DB}.t_view; DROP VIEW ${DB}.v;"

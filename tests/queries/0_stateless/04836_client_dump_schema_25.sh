#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema picks the typing settings of a CREATE as one combination that analyzes all its expressions together,
# and warns when its storage expressions also need non-default values in the server's profile.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT
cd "$work" || exit 1

# Creates `$3` with the server-level settings `$2`, dumps database d and replays the dump, both with the same settings.
replay()
{
    local name=$1 settings=$2 sql=$3
    rm -rf "$work/$name"
    # shellcheck disable=SC2086
    $CLICKHOUSE_LOCAL --path "$work/$name/src" $settings -n -q "CREATE DATABASE d; $sql" || return
    # shellcheck disable=SC2086
    $CLICKHOUSE_LOCAL --path "$work/$name/src" $settings --dump-schema d > "$work/$name/dump.sql" 2> "$work/$name/err.txt" \
        || { echo "$name: dump failed: $(cat "$work/$name/err.txt")"; return; }
    grep -h 'typing\|supertype\|function_tuple' "$work/$name/err.txt"
    grep '^SET allow_lossy\|^SET enable_named' "$work/$name/dump.sql"
    # shellcheck disable=SC2086
    $CLICKHOUSE_LOCAL --path "$work/$name/dst" $settings -n --queries-file "$work/$name/dump.sql" > "$work/$name/out.txt" 2>&1
    echo "$name: replay rc=$? $(head -c 200 "$work/$name/out.txt")"
}

decimal_sum="arraySum([toDecimal64(a, 2), 0.5])"

echo '--- column defaults need both settings at once ---'
# Each setting alone fails on the other column, so the default session's 0 must not be kept for either.
$CLICKHOUSE_LOCAL --path "$work/pair/src" -n -q "CREATE DATABASE d;
    SET allow_lossy_numeric_supertype = 1, enable_named_columns_in_function_tuple = 1;
    CREATE TABLE d.t (a UInt64, x Float64 DEFAULT $decimal_sum, y UInt64 DEFAULT tupleElement(tuple(a), 'a'))
    ENGINE = MergeTree ORDER BY a;"
$CLICKHOUSE_LOCAL --path "$work/pair/src" --dump-schema d > "$work/pair/dump.sql" 2> "$work/pair/err.txt" \
    || echo "pair: dump failed: $(cat "$work/pair/err.txt")"
grep '^SET allow_lossy\|^SET enable_named' "$work/pair/dump.sql"
$CLICKHOUSE_LOCAL --path "$work/pair/dst" -n --queries-file "$work/pair/dump.sql" > "$work/pair/out.txt" 2>&1
echo "pair: replay rc=$? $(head -c 200 "$work/pair/out.txt")"

echo '--- storage expressions ---'
lossy="--allow_lossy_numeric_supertype 1"
# The view's SELECT analyzes either way; only the inner sorting key needs the setting.
replay mv_inner_key "$lossy" "CREATE TABLE d.src (a UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW d.mv ENGINE = MergeTree ORDER BY $decimal_sum AS SELECT a FROM d.src;"
# CREATE analyzes the storage expressions with the session's settings too, so the SET must make them analyze.
replay default_and_key "$lossy" "CREATE TABLE d.t (a UInt64, b UInt8 DEFAULT if(a > 0, 1, 2)) ENGINE = MergeTree ORDER BY $decimal_sum;"
replay index "$lossy" "CREATE TABLE d.t (a UInt64, INDEX i $decimal_sum TYPE minmax) ENGINE = MergeTree ORDER BY a;"
replay projection "$lossy" "CREATE TABLE d.t (a UInt64, PROJECTION p (SELECT a ORDER BY $decimal_sum)) ENGINE = MergeTree ORDER BY a;"
replay projection_with "$lossy" "CREATE TABLE d.t (a UInt64, PROJECTION p (WITH $decimal_sum AS s SELECT a ORDER BY s)) ENGINE = MergeTree ORDER BY a;"
replay ttl "$lossy" "CREATE TABLE d.t (a UInt64, d DateTime) ENGINE = MergeTree ORDER BY a TTL d WHERE $decimal_sum > 0;"

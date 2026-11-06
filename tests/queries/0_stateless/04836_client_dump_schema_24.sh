#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema keeps MergeTree parts and FileLog offsets as they were, and its dumps replay: the typing settings
# of stored expressions, a computed sorting-key type, a constraint subquery and an `eval` construction setting.

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

# Every directory, and every file with its checksum and size, of the data path `$1`, except the `status` file.
snapshot()
{
    (cd "$1" && find . -mindepth 1 -type d | LC_ALL=C sort && find . -type f ! -name status -exec cksum {} + | LC_ALL=C sort -k3)
}

echo '--- a dump changes no part and no FileLog offset ---'
src="$work/src"
# clickhouse-local reads user files from its working directory.
mkdir -p "$work/logs"
printf 'a\nb\n' > "$work/logs/app.log"
$CLICKHOUSE_LOCAL --path "$src" -n -q "
CREATE DATABASE d;
CREATE TABLE d.mt (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO d.mt VALUES (1);
INSERT INTO d.mt VALUES (2);
CREATE TABLE d.fl (line String) ENGINE = FileLog('logs/', 'LineAsString');
SELECT count() FROM d.fl SETTINGS stream_like_engine_allow_direct_select = 1 FORMAT Null;
"
mt_dir=$(dirname "$(find "$src/store" -name format_version.txt | head -1)")
# A broken part, which loading detaches, and no format_version.txt or detached directory, which loading creates.
rm "$(find "$mt_dir/all_1_1_0" -name '*.bin' | head -1)"
rm "$mt_dir/format_version.txt"
rm -rf "$mt_dir/detached"
# A rotated log file, whose offset file loading renames, and a removed one, whose offset file loading removes.
mv "$work/logs/app.log" "$work/logs/app.log.1"
printf 'c\n' > "$work/logs/other.log"
before=$(snapshot "$src")
$CLICKHOUSE_LOCAL --path "$src" --dump-schema d > "$work/dump.sql" 2> "$work/err.txt" || echo "FAIL: dump failed: $(cat "$work/err.txt")"
[ "$before" = "$(snapshot "$src")" ] && echo "data path unchanged" || diff <(echo "$before") <(snapshot "$src")

# Creates `$2` in a fresh data path with the settings `$3...`, dumps database d and replays the dump into another one.
replay()
{
    local name=$1 sql=$2
    shift 2
    rm -rf "$work/$name"
    $CLICKHOUSE_LOCAL --path "$work/$name/src" "$@" -n -q "CREATE DATABASE d; $sql" || return
    $CLICKHOUSE_LOCAL --path "$work/$name/src" --dump-schema d > "$work/$name/dump.sql" 2> "$work/$name/err.txt" \
        || { echo "$name: dump failed: $(cat "$work/$name/err.txt")"; return; }
    $CLICKHOUSE_LOCAL --path "$work/$name/dst" -n --queries-file "$work/$name/dump.sql" > "$work/$name/out.txt" 2>&1
    echo "$name: replay rc=$? $(head -c 200 "$work/$name/out.txt")"
}

echo '--- dumps replay ---'
replay lossy_supertype "CREATE TABLE d.t (a UInt32, x Float64 DEFAULT arraySum([toDecimal64(a, 2), 0.5])) ENGINE = MergeTree ORDER BY a;" \
    --allow_lossy_numeric_supertype 1
replay named_tuple "CREATE TABLE d.t (x UInt64, y UInt64 DEFAULT tupleElement(tuple(x), 'x')) ENGINE = MergeTree ORDER BY x;" \
    --enable_named_columns_in_function_tuple 1
replay computed_key_type "CREATE TABLE d.t (x UInt64) ENGINE = MergeTree ORDER BY CAST(x, concat('Simple', 'AggregateFunction(sum, UInt64)'));" \
    --allow_suspicious_primary_key 1
replay eval_filter "CREATE TABLE d.zzz_src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE d.aaa (x UInt64) ENGINE = Remote('127.0.0.1', eval('SELECT toUInt64(1) AS x SETTINGS filter = ''x IN (SELECT x FROM d.zzz_src)'''));" \
    --allow_experimental_eval_table_function 1

# clickhouse-local cannot load a table whose constraint runs a subquery, so this dump is taken from the server.
$CLICKHOUSE_CLIENT -n -q "
CREATE TABLE zzz_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE aaa (id UInt64, CONSTRAINT c CHECK id <= (SELECT max(id) FROM merge('${CLICKHOUSE_DATABASE}', '^zzz_src\$'))) ENGINE = MergeTree ORDER BY id;"
$CLICKHOUSE_CLIENT --dump-schema "${CLICKHOUSE_DATABASE}" > "$work/constraint.sql" 2> "$work/constraint.err" \
    || echo "constraint: dump failed: $(cat "$work/constraint.err")"
$CLICKHOUSE_LOCAL --path "$work/constraint_dst" -n --queries-file "$work/constraint.sql" > "$work/constraint.out" 2>&1
echo "constraint_subquery: replay rc=$? $(head -c 200 "$work/constraint.out")"

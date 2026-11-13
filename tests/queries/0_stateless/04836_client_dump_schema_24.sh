#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: the dump of a `Replicated` database names macros that the replaying `clickhouse-local` does not have.

# --dump-schema keeps MergeTree parts, their transaction files and FileLog offsets as they were, and its dumps replay: the typing settings
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
printf 'e\n' > "$work/logs/empty.log"
$CLICKHOUSE_LOCAL --path "$src" -n -q "
CREATE DATABASE d;
CREATE TABLE d.mt (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO d.mt VALUES (1);
INSERT INTO d.mt VALUES (2);
INSERT INTO d.mt VALUES (3);
INSERT INTO d.mt VALUES (4);
CREATE TABLE d.fl (line String) ENGINE = FileLog('logs/', 'LineAsString');
SELECT count() FROM d.fl SETTINGS stream_like_engine_allow_direct_select = 1 FORMAT Null;
"
mt_dir=$(dirname "$(find "$src/store" -name format_version.txt | head -1)")
# A broken part, which loading detaches, and no format_version.txt or detached directory, which loading creates.
rm "$(find "$mt_dir/all_1_1_0" -name '*.bin' | head -1)"
rm "$mt_dir/format_version.txt"
rm -rf "$mt_dir/detached"
# A leftover temporary transaction metadata file, which loading removes.
: > "$mt_dir/all_2_2_0/txn_version.txt.tmp"
# A part without columns.txt and one with an empty checksums.txt, which loading writes again.
rm "$mt_dir/all_3_3_0/columns.txt"
: > "$mt_dir/all_4_4_0/checksums.txt"
# An empty offset file, which loading removes.
: > "$(find "$src/store" -path '*/metadata/empty.log' | head -1)"
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
# Valid with the setting off, its default would type as Float64 and fail with it on.
replay lossy_supertype_off "CREATE TABLE d.t (a UInt64, x String DEFAULT toString(variantType(if(a > 0, toDecimal64(a, 2), 0.5)))) ENGINE = MergeTree ORDER BY a;"
replay named_tuple "CREATE TABLE d.t (x UInt64, y UInt64 DEFAULT tupleElement(tuple(x), 'x')) ENGINE = MergeTree ORDER BY x;" \
    --enable_named_columns_in_function_tuple 1
replay computed_key_type "CREATE TABLE d.t (x UInt64) ENGINE = MergeTree ORDER BY CAST(x, concat('Simple', 'AggregateFunction(sum, UInt64)'));" \
    --allow_suspicious_primary_key 1
replay eval_filter "CREATE TABLE d.zzz_src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE d.aaa (x UInt64) ENGINE = Remote('127.0.0.1', eval('SELECT toUInt64(1) AS x SETTINGS filter = ''x IN (SELECT x FROM d.zzz_src)'''));" \
    --allow_experimental_eval_table_function 1

echo '--- a probe does not read the file of an ALIAS column ---'
echo hi > "$work/payload.txt"
$CLICKHOUSE_LOCAL --path "$work/alias" -n -q "
CREATE DATABASE d;
CREATE TABLE d.m (k UInt64, a String ALIAS file('payload.txt')) ENGINE = Memory;
CREATE MATERIALIZED VIEW d.mv ENGINE = Memory AS SELECT a FROM d.m;"
# A FIFO without a writer blocks whoever reads it.
rm "$work/payload.txt"
mkfifo "$work/payload.txt"
timeout 60 $CLICKHOUSE_LOCAL --path "$work/alias" --dump-schema d > /dev/null 2>&1
echo "alias_file: dump rc=$?"

# clickhouse-local cannot load a table whose constraint runs a subquery, so this dump is taken from the server.
$CLICKHOUSE_CLIENT -n -q "
CREATE TABLE zzz_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE aaa (id UInt64, CONSTRAINT c CHECK id <= (SELECT max(id) FROM merge('${CLICKHOUSE_DATABASE}', '^zzz_src\$'))) ENGINE = MergeTree ORDER BY id;"
$CLICKHOUSE_CLIENT --dump-schema "${CLICKHOUSE_DATABASE}" > "$work/constraint.sql" 2> "$work/constraint.err" \
    || echo "constraint: dump failed: $(cat "$work/constraint.err")"
$CLICKHOUSE_LOCAL --path "$work/constraint_dst" -n --queries-file "$work/constraint.sql" > "$work/constraint.out" 2>&1
echo "constraint_subquery: replay rc=$? $(head -c 200 "$work/constraint.out")"

# The only analyzer gate of this table is in the subquery of its constraint, which CREATE runs.
$CLICKHOUSE_CLIENT -n -q "
SET allow_suspicious_types_in_group_by = 1;
CREATE TABLE gate_in_constraint (id UInt64,
    CONSTRAINT c CHECK id <= (SELECT count() FROM numbers(1) GROUP BY CAST(number AS Variant(UInt64, String)))) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --dump-schema "${CLICKHOUSE_DATABASE}" > "$work/gate.sql" 2> "$work/gate.err" \
    || echo "constraint_gate: dump failed: $(cat "$work/gate.err")"
echo "constraint_gate: gate emitted: $(grep -c '^SET allow_suspicious_types_in_group_by = 1;' "$work/gate.sql")"
$CLICKHOUSE_LOCAL --path "$work/gate_dst" -n --queries-file "$work/gate.sql" > "$work/gate.out" 2>&1
echo "constraint_gate: replay rc=$? $(head -c 200 "$work/gate.out")"

# A dictionary with `dictionary_lazy_load = 0` loads its source when it is attached, unless the dump stops it.
$CLICKHOUSE_LOCAL --path "$work/dict" -n -q "
CREATE DATABASE d;
CREATE DICTIONARY d.eager (id UInt64, val String) PRIMARY KEY id
    SOURCE(EXECUTABLE(COMMAND 'touch $work/dict_marker' FORMAT 'TabSeparated'))
    LAYOUT(FLAT()) LIFETIME(0) SETTINGS(dictionary_lazy_load = 0);"
rm -f "$work/dict_marker"
$CLICKHOUSE_LOCAL --path "$work/dict" --dump-schema d > /dev/null 2> "$work/dict.err" \
    || echo "eager_dictionary: dump failed: $(cat "$work/dict.err")"
echo "eager_dictionary: source loaded: $([ -e "$work/dict_marker" ] && echo 1 || echo 0)"

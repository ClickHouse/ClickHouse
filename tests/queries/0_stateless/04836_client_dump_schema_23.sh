#!/usr/bin/env bash
# Tags: use-rocksdb

# --dump-schema in clickhouse-local leaves the data path as it was, also where loading repairs or cleans up files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Every directory and symlink, and every file with its checksum and size, of the data path `$1`, except the `status`
# file that each run rewrites.
snapshot()
{
    (cd "$1" && find . -mindepth 1 \( -type d -o -type l \) | LC_ALL=C sort && find . -type f ! -name status -exec cksum {} + | LC_ALL=C sort -k3)
}

echo '--- a dump changes no file of the data path ---'
src="$work/src"
cd "$work" || exit 1
$CLICKHOUSE_LOCAL --path "$src" --allow_deprecated_database_ordinary=1 -n -q "
CREATE DATABASE d;
CREATE TABLE d.rocks (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO d.rocks VALUES (1, 'a');
CREATE TABLE d.lg (x UInt64) ENGINE = Log;
CREATE TABLE d.tl (x UInt64) ENGINE = TinyLog;
CREATE TABLE d.sl (x UInt64) ENGINE = StripeLog;
INSERT INTO d.lg VALUES (1);
INSERT INTO d.tl VALUES (1);
INSERT INTO d.sl VALUES (1);
CREATE TABLE d.mt (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE DATABASE o ENGINE = Ordinary;
CREATE TABLE o.t (x UInt64) ENGINE = Memory;
CREATE DATABASE \`.tmp_restore_db_for_table_dropping_d\`;
"
# The tail of an interrupted INSERT, which loading truncates.
for f in $(find "$src/store" -name '*.bin'); do printf 'abc' >> "$f"; done
mkdir -p "$src/flags"
touch "$src/flags/force_restore_data"
# Loading a MergeTree table with this flag rewrites its definition as ReplicatedMergeTree.
touch "$(find "$src/store" -name format_version.txt -path '*' -exec dirname {} \; | head -1)/convert_to_replicated"
# Temporary metadata files of a table, of a database, and of a drop that left no data, which loading removes.
cp "$src/metadata/d/lg.sql" "$src/metadata/d/half.sql.tmp"
echo 'x' > "$src/metadata/d/m.tmp_move_from"
cp "$src/metadata/d.sql" "$src/metadata/e.sql.tmp"
cp "$src/metadata/o/t.sql" "$src/metadata/o/gone.sql.tmp_drop"

before=$(snapshot "$src")
$CLICKHOUSE_LOCAL --path "$src" --dump-schema=d,o > "$work/dump.sql" 2> "$work/err.txt" || echo "FAIL: dump failed: $(cat "$work/err.txt")"
echo "tables dumped: $(grep -c '^CREATE TABLE' "$work/dump.sql")"
after=$(snapshot "$src")
if [ "$before" = "$after" ]; then
    echo 'data path unchanged by the dump'
else
    echo 'FAIL: the dump changed the data path:'
    diff <(echo "$before") <(echo "$after")
fi

echo '--- an object that was not dropped completely is refused ---'
# The object still has its data, so loading would restore its metadata file.
mkdir -p "$src/data/o/gone"
before=$(snapshot "$src")
$CLICKHOUSE_LOCAL --path "$src" --dump-schema=d,o > /dev/null 2> "$work/err.txt"
echo "dump rc=$?, refused: $(grep -c 'was not dropped completely' "$work/err.txt")"
after=$(snapshot "$src")
[ "$before" = "$after" ] && echo 'data path unchanged by the refused dump' || diff <(echo "$before") <(echo "$after")

echo '--- a database without its metadata file is refused ---'
rm -rf "$work/orphan"
# The symlink `metadata/x` stays; loading would record a new database with another UUID in `x.sql`.
$CLICKHOUSE_LOCAL --path "$work/orphan" -n -q "CREATE DATABASE x; CREATE TABLE x.t (a UInt8) ENGINE = Memory;"
rm "$work/orphan/metadata/x.sql"
before=$(snapshot "$work/orphan")
$CLICKHOUSE_LOCAL --path "$work/orphan" --dump-schema > /dev/null 2> "$work/err.txt"
echo "dump rc=$?, refused: $(grep -c 'it has no metadata file' "$work/err.txt")"
after=$(snapshot "$work/orphan")
[ "$before" = "$after" ] && echo 'data path unchanged by the refused dump' || diff <(echo "$before") <(echo "$after")

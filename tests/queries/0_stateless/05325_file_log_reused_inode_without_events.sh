#!/usr/bin/env bash
# A FileLog file that gets the inode number of a file deleted while the table was detached is read from its start,
# under another name and under the same name, also when the new file has a hard link under the rotated name; a file
# renamed while detached keeps its offset, and a name the glob excludes is not read. A single-file table reads its file
# from the start after it was deleted and created again with the same inode number. The file system reuses a freed
# inode number only sometimes; the output is the same either way.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
dir2=${dir}_single
rm -rf "${dir:?}" "${dir2:?}"
mkdir -p "$dir" "$dir2"

function inode_of()
{
    ls -i "$1" | awk '{print $1}'
}

# Writes $3 to the file $2 through a new file that got the inode number $1 (freed just before), if any.
# Returns 0 if the inode number was reused.
function write_with_inode()
{
    local inode=$1 target=$2 content=$3 d hit i
    d=$(dirname "$target")
    for i in {1..1000}; do : > "$d/fill_${step}_$i"; done
    hit=$(ls -i "$d" | awk -v inode="$inode" '$1 == inode && $2 ~ /^fill_/ {print $2}')
    if [[ -n "$hit" ]]; then
        printf '%s' "$content" > "$d/$hit"
        mv "$d/$hit" "$target"
    fi
    rm -f "$d"/fill_"${step}"_*
    [[ -n "$hit" ]] && return 0
    printf '%s' "$content" > "$target"
    return 1
}

# Before the table: the directory is watched only some time after CREATE returns.
printf '1\n2\n3\n' > "$dir/a.csv"
$CLICKHOUSE_CLIENT -q "CREATE TABLE file_log (v UInt64) ENGINE = FileLog('$dir/*.csv', 'CSV') SETTINGS max_threads = 1, poll_timeout_ms = 100"

step=0
# Reads until a newly created matching file is seen: then every earlier file operation has been processed.
function read_rows()
{
    step=$((step + 1))
    local rows="" i seen=0
    sleep 1
    for i in {1..300}; do
        printf '0\n' > "$dir/barrier_${step}_${i}.csv"
        rows+=$($CLICKHOUSE_CLIENT -q "SELECT _filename, v FROM file_log SETTINGS stream_like_engine_allow_direct_select = 1 FORMAT TSV")$'\n' || break
        grep -q "^barrier_${step}_" <<< "$rows" && { seen=1; break; }
        sleep 0.2
    done
    echo "-- $1"
    (( seen )) || echo "barrier ${step} not reached"
    grep -v -e '^barrier_' -e '^$' <<< "$rows" | LC_ALL=C sort
}

read_rows "initial scan"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
inode=$(inode_of "$dir/a.csv")
rm "$dir/a.csv"
write_with_inode "$inode" "$dir/b.csv" $'10\n20\n30\n40\n'
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
read_rows "other name, while detached"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
inode=$(inode_of "$dir/b.csv")
rm "$dir/b.csv"
write_with_inode "$inode" "$dir/b.csv" $'50\n60\n70\n80\n90\n'
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
read_rows "same name, while detached"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
mv "$dir/b.csv" "$dir/c.csv"
printf '100\n' >> "$dir/c.csv"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
read_rows "renamed while detached"

printf '1\n2\n' > "$dir/d.csv"
read_rows "file to be deleted"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
inode=$(inode_of "$dir/d.csv")
rm "$dir/d.csv"
write_with_inode "$inode" "$dir/x.log" $'7\n8\n9\n'
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
read_rows "name the glob excludes, while detached"
read_rows "name the glob excludes, next read"

printf '1\n2\n' > "$dir/e.csv"
read_rows "file to be rotated"
mv "$dir/e.csv" "$dir/e.csv.1"
read_rows "rotated"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
inode=$(inode_of "$dir/e.csv.1")
rm "$dir/e.csv.1"
write_with_inode "$inode" "$dir/f.csv" $'11\n12\n13\n'
ln "$dir/f.csv" "$dir/e.csv.1"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
read_rows "hard link under the rotated name, while detached"

$CLICKHOUSE_CLIENT -q "DROP TABLE file_log"

echo "-- single-file table"
printf '1\n2\n3\n' > "$dir2/single.csv"
$CLICKHOUSE_CLIENT -q "CREATE TABLE file_log_single (v UInt64) ENGINE = FileLog('$dir2/single.csv', 'CSV') SETTINGS poll_timeout_ms = 100"
$CLICKHOUSE_CLIENT -q "SELECT _filename, v FROM file_log_single ORDER BY v SETTINGS stream_like_engine_allow_direct_select = 1 FORMAT TSV"

inode=$(inode_of "$dir2/single.csv")
rm "$dir2/single.csv"
step=single
if ! write_with_inode "$inode" "$dir2/single.csv" $'10\n20\n30\n40\n'; then
    # A single-file table reads a file with another inode number only after it is loaded again.
    $CLICKHOUSE_CLIENT -q "DETACH TABLE file_log_single"
    $CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log_single"
fi
echo "-- single-file table, deleted and created again"
$CLICKHOUSE_CLIENT -q "SELECT _filename, v FROM file_log_single ORDER BY v SETTINGS stream_like_engine_allow_direct_select = 1 FORMAT TSV"

$CLICKHOUSE_CLIENT -q "DROP TABLE file_log_single"
rm -rf "${dir:?}" "${dir2:?}"

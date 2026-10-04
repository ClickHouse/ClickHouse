#!/usr/bin/env bash
# Tags: long
# A glob in the file name of the FileLog path selects which files of the directory are read.
# A file that is already read keeps being read after it is renamed to a non-matching name (log rotation),
# also across DETACH/ATTACH, until it is removed.
# A file renamed from a matching name before the table read it is read, also after a second rename, and a rotation chain renamed while detached keeps its offsets.
# An unread file never becomes read by passing through a read name, and a read file renamed over a new file keeps its offset.
# A hard link to a read file is not read again, also under a name the glob matches (also for a rotated file), and removing it does not stop the reading.
# An existing directory whose name has glob characters is taken literally.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
outside=${dir}_outside
braces="${dir}_{old}"
rm -rf "${dir:?}" "${outside:?}" "${braces:?}"
mkdir -p "$dir" "$outside" "$braces"

printf '1\n2\n' > "$dir/app.log"
printf '100\n' > "$dir/notes.txt"
printf '200\n' > "$dir/app.log.1.gz"

$CLICKHOUSE_CLIENT -q "CREATE TABLE file_log (v UInt64) ENGINE = FileLog('$dir/*.log', 'TSV') SETTINGS max_threads = 1, poll_timeout_ms = 100"

step=0
# Reads until a newly created matching file is seen: then every earlier file operation has been processed.
function read_rows()
{
    step=$((step + 1))
    local rows="" i
    # A file is opened by name, so give the watcher time to deliver a rename before the first read.
    sleep 1
    for i in {1..300}; do
        printf '0\n' > "$dir/barrier_${step}_${i}.log"
        rows+=$($CLICKHOUSE_CLIENT -q "SELECT _filename, v FROM file_log SETTINGS stream_like_engine_allow_direct_select = 1 FORMAT TSV")$'\n' || break
        grep -q "^barrier_${step}_" <<< "$rows" && break
        sleep 0.2
    done
    echo "-- $1"
    grep -v -e '^barrier_' -e '^$' <<< "$rows" | LC_ALL=C sort
}

read_rows "initial scan"

printf '3\n' >> "$dir/app.log"
printf '4\n' > "$dir/new.log"
printf '300\n' > "$dir/new.txt"
printf '5\n' > "$outside/moved_in.log"; mv "$outside/moved_in.log" "$dir/"
printf '400\n' > "$outside/moved_in.gz"; mv "$outside/moved_in.gz" "$dir/"
read_rows "new files"

printf '6\n' >> "$dir/app.log"
mv "$dir/app.log" "$dir/app.log.1"
printf '7\n' >> "$dir/app.log.1"
printf '8\n' > "$dir/app.log"
read_rows "rotation"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
printf '9\n' >> "$dir/app.log.1"
printf '10\n' >> "$dir/app.log"
mv "$dir/app.log" "$dir/app.log.0"
printf '11\n' > "$dir/app.log"
printf '500\n' > "$dir/detached.txt"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
read_rows "rotation while detached"

mv "$dir/app.log.1" "$dir/app.log.2"
printf '12\n' >> "$dir/app.log.2"
read_rows "second rotation"

printf '600\n' > "$dir/app.log.2.gz"
rm "$dir/app.log.2"
printf '13\n' >> "$dir/app.log"
read_rows "compression"

printf '700\n' > "$outside/replacement"
mv "$outside/replacement" "$dir/app.log.0"
printf '14\n' >> "$dir/app.log.0"
read_rows "renamed file replaced"

printf '15\n' > "$dir/fresh.log"
printf '17\n' > "$dir/hop.log"
# The macOS watcher compares directory listings: it has to list both files before they are renamed.
sleep 1
mv "$dir/fresh.log" "$dir/fresh.log.1"
mv "$dir/hop.log" "$dir/hop.log.1"
mv "$dir/hop.log.1" "$dir/hop.log.2"
printf '16\n' >> "$dir/fresh.log.1"
read_rows "renamed before it was read"

printf '20\n' > "$dir/chain.log"
read_rows "chain"
mv "$dir/chain.log" "$dir/chain.log.1"
printf '2100\n' > "$dir/chain.log"
read_rows "chain rotation"
mv "$dir/chain.log.1" "$dir/chain.log.2"
mv "$dir/chain.log" "$dir/chain.log.1"
printf '220000\n' > "$dir/chain.log"
read_rows "chain second rotation"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
mv "$dir/chain.log.2" "$dir/chain.log.3"
mv "$dir/chain.log.1" "$dir/chain.log.2"
mv "$dir/chain.log" "$dir/chain.log.1"
printf '23\n' > "$dir/chain.log"
printf '24\n' >> "$dir/chain.log.1"
printf '25\n' >> "$dir/chain.log.3"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
read_rows "chain rotation while detached"

printf '800\n' > "$outside/replacement2"
mv "$outside/replacement2" "$dir/chain.log.3"
mv "$dir/chain.log.3" "$dir/chain.log.9"
printf '26\n' >> "$dir/chain.log.9"
printf '27\n' >> "$dir/chain.log.2"
read_rows "replaced and renamed again"

printf '50\n' > "$dir/new2.log"
mv "$dir/chain.log.1" "$dir/new2.log"
printf '29\n' >> "$dir/new2.log"
read_rows "renamed over a new file"

printf '40\n' > "$dir/gate.log"
# The macOS watcher compares directory listings: it has to list the file before it is renamed.
sleep 1
mv "$dir/gate.log" "$dir/gate.log.1"
mv "$dir/gate.log.1" "$dir/gate.log.2"
printf '900\n' > "$outside/other"
mv "$outside/other" "$dir/gate.log.1"
read_rows "renamed twice, first name refilled"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
ln "$dir/app.log" "$dir/app.log.bak"
ln "$dir/chain.log.2" "$dir/chain.log.2.bak"
printf '30\n' >> "$dir/app.log"
printf '31\n' >> "$dir/chain.log.2"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
read_rows "hard links while detached"

mv "$dir/chain.log.2.bak" "$dir/chain.log.5"
printf '32\n' >> "$dir/chain.log.2"
read_rows "hard link renamed"

ln "$dir/app.log" "$dir/alias.log"
mv "$dir/alias.log" "$dir/alias.bak"
printf '33\n' >> "$dir/app.log"
read_rows "matching hard link renamed"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
ln "$dir/chain.log.2" "$dir/chain.log.0"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
rm "$dir/chain.log.0"
printf '34\n' >> "$dir/chain.log.2"
read_rows "earlier sorting hard link removed after attach"

ln "$dir/app.log" "$dir/alias2.bak"
mv "$dir/alias2.bak" "$dir/alias2.log"
ln "$dir/app.log" "$dir/link.log"
printf '35\n' >> "$dir/app.log"
read_rows "hard link renamed to a matching name"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
ln "$dir/app.log" "$dir/a_link.log"
printf '36\n' >> "$dir/app.log"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
read_rows "matching hard link while detached"

$CLICKHOUSE_CLIENT -q "DETACH TABLE file_log"
ln "$dir/chain.log.2" "$dir/chain_alias.log"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE file_log"
rm "$dir/chain_alias.log"
printf '37\n' >> "$dir/chain.log.2"
read_rows "matching hard link to a rotated file removed after attach"

echo "-- directory name with glob characters"
printf '1\n' > "$braces/a.log"
printf '2\n' > "$braces/a.log.1.gz"
$CLICKHOUSE_CLIENT -q "CREATE TABLE file_log_braces (v UInt64) ENGINE = FileLog('$braces/*.log', 'TSV') SETTINGS max_threads = 1, poll_timeout_ms = 100"
$CLICKHOUSE_CLIENT -q "SELECT _filename, v FROM file_log_braces SETTINGS stream_like_engine_allow_direct_select = 1 FORMAT TSV"
$CLICKHOUSE_CLIENT -q "DROP TABLE file_log_braces"

echo "-- errors"
$CLICKHOUSE_CLIENT -q "CREATE TABLE file_log_bad (v UInt64) ENGINE = FileLog('$dir/*/app.log', 'TSV')" 2>&1 | grep -q 'Globs are supported only in the file name of the path' && echo OK || echo FAIL
$CLICKHOUSE_CLIENT -q "CREATE TABLE file_log_bad (v UInt64) ENGINE = FileLog('${dir}_missing/*.log', 'TSV')" 2>&1 | grep -q '_missing of the path .* does not exist' && echo OK || echo FAIL

$CLICKHOUSE_CLIENT -q "DROP TABLE file_log"
rm -rf "${dir:?}" "${outside:?}" "${braces:?}"

#!/usr/bin/env bash
# A glob in the file name of the FileLog path selects which files of the directory are read.
# A file that is already read keeps being read after it is renamed to a non-matching name (log rotation),
# also across DETACH/ATTACH, until it is removed.
# A file renamed from a matching name before the table read it is read, and a rotation chain renamed while detached keeps its offsets.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}
outside=${dir}_outside
rm -rf "${dir:?}" "${outside:?}"
mkdir -p "$dir" "$outside"

printf '1\n2\n' > "$dir/app.log"
printf '100\n' > "$dir/notes.txt"
printf '200\n' > "$dir/app.log.1.gz"

$CLICKHOUSE_CLIENT -q "CREATE TABLE file_log (v UInt64) ENGINE = FileLog('$dir/*.log', 'TSV')"

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
mv "$dir/fresh.log" "$dir/fresh.log.1"
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

echo "-- errors"
$CLICKHOUSE_CLIENT -q "CREATE TABLE file_log_bad (v UInt64) ENGINE = FileLog('$dir/*/app.log', 'TSV')" 2>&1 | grep -q 'Globs are supported only in the file name of the path' && echo OK || echo FAIL
$CLICKHOUSE_CLIENT -q "CREATE TABLE file_log_bad (v UInt64) ENGINE = FileLog('${dir}_missing/*.log', 'TSV')" 2>&1 | grep -q '_missing of the path .* does not exist' && echo OK || echo FAIL

$CLICKHOUSE_CLIENT -q "DROP TABLE file_log"
rm -rf "${dir:?}" "${outside:?}"

#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: FileLog requires inotify.

# The directories of all `FileLog` tables are watched by one shared thread. A table whose directory changes all the
# time must not keep that thread from adding and removing the watchers of the other tables: creating and dropping an
# idle table must finish while another table keeps getting events.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

busy_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/busy
idle_dir=${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/idle

rm -rf "${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${busy_dir}" "${idle_dir}"
trap 'kill "${writer}" 2>/dev/null; rm -rf "${USER_FILES_PATH:?}/${CLICKHOUSE_TEST_UNIQUE_NAME}"' EXIT

for i in {1..8}; do
    echo "${i}" > "${busy_dir}/${i}.tsv"
done
echo "1" > "${idle_dir}/a.tsv"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE busy (v String) ENGINE = FileLog('${busy_dir}/', 'TSV')"

# Keeps both the files and the directory of `busy` changing, so the watcher always has events.
(
    n=0
    start=$EPOCHSECONDS
    while ((EPOCHSECONDS - start < 120)); do
        for i in {1..8}; do
            echo "x" >> "${busy_dir}/${i}.tsv"
        done
        n=$((n + 1))
        echo "x" > "${busy_dir}/tmp_${n}"
        mv "${busy_dir}/tmp_${n}" "${busy_dir}/renamed_${n}"
        rm -f "${busy_dir}/renamed_${n}"
    done
) &
writer=$!

# The commands of the watch thread wait for it, so a starved thread makes `CREATE` or `DROP` hang, and the client
# gives up waiting for the reply.
for _ in {1..5}; do
    ${CLICKHOUSE_CLIENT} --receive_timeout 60 --query "CREATE TABLE idle (v String) ENGINE = FileLog('${idle_dir}/', 'TSV')"
    echo "create: $?"
    # Let the watcher of the idle table make its scans while the other directory changes.
    sleep 0.5
    ${CLICKHOUSE_CLIENT} --receive_timeout 60 --query "DROP TABLE idle SYNC"
    echo "drop: $?"
done

kill "${writer}"
wait "${writer}" 2>/dev/null

${CLICKHOUSE_CLIENT} --query "DROP TABLE busy SYNC"
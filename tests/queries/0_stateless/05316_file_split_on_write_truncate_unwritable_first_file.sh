#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A truncating insert deletes the numbered tail of the previous split insert, but only after it has opened
# the first file it writes. If it cannot open that file, the insert fails and the old numbered files stay.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

echo '--- The table remembers its numbered tail'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/table/data.tsv')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}"
ls "${DIR}/table" | sort
rm "${DIR}/table/data.tsv"
mkdir "${DIR}/table/data.tsv"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1" 2>&1 | grep -o -m1 'CANNOT_OPEN_FILE'
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1" 2>&1 | grep -o -m1 'CANNOT_OPEN_FILE'
ls "${DIR}/table" | sort
${CLICKHOUSE_CLIENT} --query "DROP TABLE test"

echo '--- The table function claims the numbered sequence'
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DIR}/function/data.tsv', TSV, 'x UInt64') SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}"
ls "${DIR}/function" | sort
# A dangling symlink is not a directory, so the insert picks this name and then fails to open it.
rm "${DIR}/function/data.tsv"
ln -s "${DIR}/missing_directory/target.tsv" "${DIR}/function/data.tsv"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DIR}/function/data.tsv', TSV, 'x UInt64') SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1" 2>&1 | grep -o -m1 'FILE_DOESNT_EXIST'
ls "${DIR}/function" | sort

echo '--- A partitioned insert claims the numbered sequence of the partition'
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DIR}/partitioned/data_{_partition_id}.tsv', TSV, 'x UInt64') PARTITION BY 'p' SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}"
ls "${DIR}/partitioned" | sort
rm "${DIR}/partitioned/data_p.tsv"
ln -s "${DIR}/missing_directory/target.tsv" "${DIR}/partitioned/data_p.tsv"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DIR}/partitioned/data_{_partition_id}.tsv', TSV, 'x UInt64') PARTITION BY 'p' SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1" 2>&1 | grep -o -m1 'FILE_DOESNT_EXIST'
ls "${DIR}/partitioned" | sort

echo '--- The path of the table function expands into no files'
mkdir "${DIR}/empty"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DIR}/empty', TSV, 'x UInt64') SELECT number FROM numbers(100) SETTINGS ${SETTINGS}" 2>&1 | grep -o -m1 'INCORRECT_FILE_NAME'
ls "${DIR}/empty" | sort

rm -rf "${DIR}"

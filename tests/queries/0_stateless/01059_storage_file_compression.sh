#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
WORKING_FOLDER_01059="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}"

rm -rf "${WORKING_FOLDER_01059}"
mkdir "${WORKING_FOLDER_01059}"

for m in gz br xz zst lz4 bz2
do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS file"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE file (x UInt64) ENGINE = File(TSV, '${WORKING_FOLDER_01059}/${m}.tsv.${m}')"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO file SELECT * FROM numbers(1000000)"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), max(x) FROM file"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE file"
done

${CLICKHOUSE_CLIENT} --query "SELECT count(), max(x) FROM file('${WORKING_FOLDER_01059}/{gz,br,xz,zst,lz4,bz2}.tsv.{gz,br,xz,zst,lz4,bz2}', TSV, 'x UInt64')"

for m in gz br xz zst lz4 bz2
do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() < 4000000, max(x) FROM file('${WORKING_FOLDER_01059}/${m}.tsv.${m}', RowBinary, 'x UInt8', 'none')"
done

rm -rf "${WORKING_FOLDER_01059}"
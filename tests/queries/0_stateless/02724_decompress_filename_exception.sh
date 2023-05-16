#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILENAME="${USER_FILES_PATH}/corrupted_file.tsv.xx"

echo 'corrupted file' > $FILENAME;

$CLICKHOUSE_CLIENT --query "SELECT * FROM file('${FILENAME}', 'TSV', 'c UInt32', 'gzip')" 2>&1    | grep -q "While reading from: $FILENAME" && echo 'Ok' || echo 'Fail';
$CLICKHOUSE_CLIENT --query "SELECT * FROM file('${FILENAME}', 'TSV', 'c UInt32', 'deflate')" 2>&1 | grep -q "While reading from: $FILENAME" && echo 'Ok' || echo 'Fail';
$CLICKHOUSE_CLIENT --query "SELECT * FROM file('${FILENAME}', 'TSV', 'c UInt32', 'br')" 2>&1      | grep -q "While reading from: $FILENAME" && echo 'Ok' || echo 'Fail';
$CLICKHOUSE_CLIENT --query "SELECT * FROM file('${FILENAME}', 'TSV', 'c UInt32', 'xz')" 2>&1      | grep -q "While reading from: $FILENAME" && echo 'Ok' || echo 'Fail';
$CLICKHOUSE_CLIENT --query "SELECT * FROM file('${FILENAME}', 'TSV', 'c UInt32', 'zstd')" 2>&1    | grep -q "While reading from: $FILENAME" && echo 'Ok' || echo 'Fail';
$CLICKHOUSE_CLIENT --query "SELECT * FROM file('${FILENAME}', 'TSV', 'c UInt32', 'lz4')" 2>&1     | grep -q "While reading from: $FILENAME" && echo 'Ok' || echo 'Fail';
$CLICKHOUSE_CLIENT --query "SELECT * FROM file('${FILENAME}', 'TSV', 'c UInt32', 'bz2')" 2>&1     | grep -q "While reading from: $FILENAME" && echo 'Ok' || echo 'Fail';
$CLICKHOUSE_CLIENT --query "SELECT * FROM file('${FILENAME}', 'TSV', 'c UInt32', 'snappy')" 2>&1  | grep -q "While reading from: $FILENAME" && echo 'Ok' || echo 'Fail';

rm $FILENAME;

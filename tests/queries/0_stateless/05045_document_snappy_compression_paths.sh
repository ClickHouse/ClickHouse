#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Smoke coverage for the `snappy` and `deflate` compression paths documented for `file()` / `File`,
# `INTO OUTFILE ... COMPRESSION`, `INSERT ... FROM INFILE` and `url()`.
# The object-storage table functions share the same `chooseCompressionMethod` stack as `file()`.

set -e

# Server-side paths are relative to `user_files_path`.
SERVER_REL="${CLICKHOUSE_TEST_UNIQUE_NAME}"
SERVER_ABS="${USER_FILES_PATH}/${SERVER_REL}"
mkdir -p "${SERVER_ABS}"
chmod 777 "${SERVER_ABS}"

CLIENT_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${CLIENT_DIR}"

trap 'rm -rf "${SERVER_ABS}" "${CLIENT_DIR}"' EXIT

FILE_TF_ABS="${SERVER_ABS}/tf.tsv.snappy"
INFILE_EXPLICIT="${CLIENT_DIR}/infile_explicit.tsv"
INFILE_AUTO="${CLIENT_DIR}/infile_auto.tsv.snappy"

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS snappy_file_engine;

SELECT 'file_table_function';
INSERT INTO FUNCTION file('${SERVER_REL}/tf.tsv.snappy', 'TSV', 'x UInt32', 'snappy')
SELECT number FROM numbers(5) SETTINGS engine_file_truncate_on_insert = 1;
SELECT x FROM file('${SERVER_REL}/tf.tsv.snappy', 'TSV', 'x UInt32') ORDER BY x;
SELECT x FROM file('${SERVER_REL}/tf.tsv.snappy', 'TSV', 'x UInt32', 'snappy') ORDER BY x;

SELECT 'file_table_function_deflate';
INSERT INTO FUNCTION file('${SERVER_REL}/tf.tsv.deflate', 'TSV', 'x UInt32', 'deflate')
SELECT number FROM numbers(5) SETTINGS engine_file_truncate_on_insert = 1;
SELECT x FROM file('${SERVER_REL}/tf.tsv.deflate', 'TSV', 'x UInt32') ORDER BY x;

SELECT 'file_engine';
CREATE TABLE snappy_file_engine (x UInt32) ENGINE = File(TSV, '${SERVER_REL}/engine.tsv.snappy');
INSERT INTO snappy_file_engine SELECT number FROM numbers(5);
SELECT x FROM snappy_file_engine ORDER BY x;
DROP TABLE snappy_file_engine;

SELECT 'url_file_scheme';
SELECT x FROM url('file://${FILE_TF_ABS}', 'TSV', 'x UInt32', 'snappy') ORDER BY x;
"

echo "into_outfile"
${CLICKHOUSE_CLIENT} -q "
SELECT number FROM numbers(5) INTO OUTFILE '${SERVER_ABS}/outfile.tsv.snappy' COMPRESSION 'snappy' FORMAT TSV
"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM file('${SERVER_REL}/outfile.tsv.snappy', 'TSV', 'x UInt32', 'snappy') ORDER BY x
"

# `FROM INFILE` reads on the client side, so the file has to live outside `user_files_path`.
echo "insert_from_infile"
${CLICKHOUSE_CLIENT} -q "SELECT number FROM numbers(5) INTO OUTFILE '${INFILE_EXPLICIT}' COMPRESSION 'snappy' FORMAT TSV"
${CLICKHOUSE_CLIENT} -q "SELECT number FROM numbers(5) INTO OUTFILE '${INFILE_AUTO}' COMPRESSION 'snappy' FORMAT TSV"

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS snappy_infile;
CREATE TABLE snappy_infile (x UInt32) ENGINE = Memory;
"
${CLICKHOUSE_CLIENT} -q "INSERT INTO snappy_infile FROM INFILE '${INFILE_EXPLICIT}' COMPRESSION 'snappy' FORMAT TSV"
${CLICKHOUSE_CLIENT} -q "INSERT INTO snappy_infile FROM INFILE '${INFILE_AUTO}' FORMAT TSV"
${CLICKHOUSE_CLIENT} -m -q "
SELECT x, count() FROM snappy_infile GROUP BY x ORDER BY x;
DROP TABLE snappy_infile;
"

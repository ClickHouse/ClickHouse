#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Smoke coverage for the user-facing snappy compression paths documented for
# `file()` / `File`, `INTO OUTFILE ... COMPRESSION`, and `INSERT ... FROM INFILE`.
# Object-storage table functions (`s3` / `gcs` / `azureBlobStorage`) share the same
# `chooseCompressionMethod` / snappy wrapper stack as `file()`, so they are covered
# by the `file()` round-trips below.

set -e

# Server-side paths are relative to `user_files_path` (same pattern as 01059 / 04201).
SERVER_REL="${CLICKHOUSE_TEST_UNIQUE_NAME}"
SERVER_ABS="${USER_FILES_PATH}/${SERVER_REL}"
mkdir -p "${SERVER_ABS}"
chmod 777 "${SERVER_ABS}"

CLIENT_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${CLIENT_DIR}"

trap 'rm -rf "${SERVER_ABS}" "${CLIENT_DIR}"' EXIT

FILE_TF_REL="${SERVER_REL}/tf.tsv.snappy"
FILE_TF_ABS="${SERVER_ABS}/tf.tsv.snappy"
FILE_ENGINE_REL="${SERVER_REL}/engine.tsv.snappy"
OUTFILE_ABS="${SERVER_ABS}/outfile.tsv.snappy"
INFILE_EXPLICIT="${CLIENT_DIR}/infile_explicit.tsv"
INFILE_AUTO="${CLIENT_DIR}/infile_auto.tsv.snappy"

echo "file_table_function"
${CLICKHOUSE_CLIENT} -q "
INSERT INTO FUNCTION file('${FILE_TF_REL}', 'TSV', 'x UInt32', 'snappy')
SELECT number FROM numbers(5)
SETTINGS engine_file_truncate_on_insert = 1;
"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM file('${FILE_TF_REL}', 'TSV', 'x UInt32')
ORDER BY x;
"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM file('${FILE_TF_REL}', 'TSV', 'x UInt32', 'snappy')
ORDER BY x;
"

echo "file_engine"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS snappy_file_engine"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE snappy_file_engine (x UInt32)
ENGINE = File(TSV, '${FILE_ENGINE_REL}');
"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE snappy_file_engine"
${CLICKHOUSE_CLIENT} -q "INSERT INTO snappy_file_engine SELECT number FROM numbers(5)"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM snappy_file_engine ORDER BY x"
${CLICKHOUSE_CLIENT} -q "DROP TABLE snappy_file_engine"

echo "into_outfile"
rm -f "${OUTFILE_ABS}"
${CLICKHOUSE_CLIENT} -q "
SELECT number FROM numbers(5)
INTO OUTFILE '${OUTFILE_ABS}' COMPRESSION 'snappy' FORMAT TSV;
"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM file('${SERVER_REL}/outfile.tsv.snappy', 'TSV', 'x UInt32', 'snappy')
ORDER BY x;
"

echo "insert_from_infile"
rm -f "${INFILE_EXPLICIT}" "${INFILE_AUTO}"
${CLICKHOUSE_CLIENT} -q "
SELECT number FROM numbers(5)
INTO OUTFILE '${INFILE_EXPLICIT}' COMPRESSION 'snappy' FORMAT TSV;
"
${CLICKHOUSE_CLIENT} -q "
SELECT number FROM numbers(5)
INTO OUTFILE '${INFILE_AUTO}' COMPRESSION 'snappy' FORMAT TSV;
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS snappy_infile"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE snappy_infile (x UInt32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "
INSERT INTO snappy_infile FROM INFILE '${INFILE_EXPLICIT}' COMPRESSION 'snappy' FORMAT TSV;
"
${CLICKHOUSE_CLIENT} -q "
INSERT INTO snappy_infile FROM INFILE '${INFILE_AUTO}' FORMAT TSV;
"
${CLICKHOUSE_CLIENT} -q "SELECT x, count() FROM snappy_infile GROUP BY x ORDER BY x"
${CLICKHOUSE_CLIENT} -q "DROP TABLE snappy_infile"

echo "url_file_scheme"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM url('file://${FILE_TF_ABS}', 'TSV', 'x UInt32', 'snappy')
ORDER BY x;
"

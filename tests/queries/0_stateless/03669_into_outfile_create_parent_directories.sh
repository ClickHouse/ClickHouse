#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${CUR_DIR}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
CLIENT_OUTDIR="${BASE_DIR}/client"

rm -rf "$BASE_DIR" 2>/dev/null || true

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS test_outfile;
CREATE TABLE test_outfile (id UInt32, value String) ENGINE = Memory;
INSERT INTO test_outfile VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');
SET into_outfile_create_parent_directories = 1;

SELECT * FROM test_outfile INTO OUTFILE '${CLIENT_OUTDIR}/level1/level2/output.csv' FORMAT CSV;
SELECT * FROM test_outfile INTO OUTFILE '${CLIENT_OUTDIR}/a/b/c/d/output.tsv' FORMAT TSV;
SELECT * FROM test_outfile INTO OUTFILE '${CLIENT_OUTDIR}/compressed/output.csv.gz' COMPRESSION 'gzip' FORMAT CSV;
SELECT * FROM test_outfile INTO OUTFILE '${CLIENT_OUTDIR}/formats/output.json' FORMAT JSONEachRow;
SELECT * FROM test_outfile WHERE id > 100 INTO OUTFILE '${CLIENT_OUTDIR}/empty/output.csv' FORMAT CSV;
SELECT id, value, length(value) AS len FROM test_outfile ORDER BY id
INTO OUTFILE '${CLIENT_OUTDIR}/complex/output.csv' FORMAT CSV;
SELECT * FROM test_outfile INTO OUTFILE '${CLIENT_OUTDIR}/level1/../level1_sibling/output.csv' FORMAT CSV;

DROP TABLE test_outfile;
"

rm -rf "$BASE_DIR" 2>/dev/null || true
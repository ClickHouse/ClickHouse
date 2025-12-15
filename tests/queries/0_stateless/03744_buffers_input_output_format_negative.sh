#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CLICKHOUSE_TMP"

$CLICKHOUSE_LOCAL -n <<'SQL'
SET engine_file_truncate_on_insert = 1;

SELECT 'Buffers: prepare base files for negative tests';

-- 3-column file: a, b, c
SELECT
    1 AS a,
    2 AS b,
    3 AS c
INTO OUTFILE '03744_buffers_neg_three_cols.buffers' TRUNCATE
FORMAT Buffers;

-- 2-column file: a, b
SELECT
    1 AS a,
    2 AS b
INTO OUTFILE '03744_buffers_neg_two_cols.buffers' TRUNCATE
FORMAT Buffers;

-- const file: k as UInt64, s as String
SELECT
    number AS k,
    'x'    AS s
FROM numbers(10)
INTO OUTFILE '03744_buffers_neg_const.buffers' TRUNCATE
FORMAT Buffers;

SELECT 'Buffers: Schema missing';
SELECT *
FROM file(
    '03744_buffers_neg_three_cols.buffers',
    'Buffers'
); -- { serverError BAD_ARGUMENTS }

DESCRIBE file('03744_buffers_neg_three_cols.buffers', Buffers); -- { serverError BAD_ARGUMENTS }

SELECT 'Buffers: fewer columns in header than in file';

-- File has 3 buffers (a,b,c) but structure only has 2 columns -> column-count mismatch
SELECT *
FROM file(
    '03744_buffers_neg_three_cols.buffers',
    'Buffers',
    'a UInt8, b UInt8'
); -- { serverError INCORRECT_DATA }

SELECT 'Buffers: more columns in header than in file';

-- File has 2 buffers (a,b) but structure has 3 columns -> column-count mismatch
SELECT *
FROM file(
    '03744_buffers_neg_two_cols.buffers',
    'Buffers',
    'a UInt8, b UInt8, c UInt8'
); -- { serverError INCORRECT_DATA }

SELECT 'Buffers: row count mismatch because of type mismatch';

-- k was written as UInt64 (8 bytes per row) but is read as UInt8 (1 byte per row):
-- first column infers 80 rows, second column really has 10 -> row-count mismatch
SELECT
    sum(k),
    groupArray(s)
FROM file(
    '03744_buffers_neg_const.buffers',
    'Buffers',
    'k UInt8, s String'
); -- { serverError INCORRECT_DATA }


SELECT 'Buffers: complex schema column-count mismatch';

-- Prepare a file with 3 columns: id, j, v
SELECT
    1 AS id,
    '{"a": {"b": 42}}' AS j,
    42 AS v
INTO OUTFILE '03744_buffers_neg_complex.buffers' TRUNCATE
FORMAT Buffers;

-- Try to read it as only 2 columns -> column-count mismatch
SELECT *
FROM file(
    '03744_buffers_neg_complex.buffers',
    'Buffers',
    'id UInt8, j JSON(a.b UInt32)'
); -- { serverError INCORRECT_DATA }
SQL

rm -f 03744_buffers_neg_*.buffers

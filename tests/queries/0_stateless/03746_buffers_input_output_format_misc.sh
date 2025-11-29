#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CLICKHOUSE_TMP"

# File engine with Buffers

# Simple one-column File(Buffers) table
$CLICKHOUSE_CLIENT -n <<SQL
DROP TABLE IF EXISTS file_buffers_simple;

CREATE TABLE file_buffers_simple
(
    x UInt64
)
ENGINE = File(Buffers, '${CLICKHOUSE_DATABASE}/03746_file_engine_buffers_simple.data');

INSERT INTO file_buffers_simple
SELECT number
FROM numbers(10);

SELECT 'File(Buffers) simple sum';
SELECT sum(x) FROM file_buffers_simple;

DROP TABLE IF EXISTS file_buffers_simple_clone;
CREATE TABLE file_buffers_simple_clone
(
    x UInt64
)
ENGINE = File(Buffers, '${CLICKHOUSE_DATABASE}/03746_file_engine_buffers_simple.data');

SELECT 'original', sum(x) FROM file_buffers_simple;
SELECT 'clone   ', sum(x) FROM file_buffers_simple_clone;
SQL

# Two-column File(Buffers) table

$CLICKHOUSE_CLIENT -n <<SQL
DROP TABLE IF EXISTS file_buffers_two_cols;

CREATE TABLE file_buffers_two_cols
(
    id UInt64,
    k  UInt8
)
ENGINE = File(Buffers, '${CLICKHOUSE_DATABASE}/03746_file_engine_buffers_two_cols.data');

INSERT INTO file_buffers_two_cols
SELECT
    number AS id,
    number % 3 AS k
FROM numbers(10);

SELECT 'File(Buffers) two-cols aggregate';
SELECT
    count()  AS cnt,
    sum(id)  AS sum_id,
    sum(k)   AS sum_k
FROM file_buffers_two_cols;
SQL

# EPHEMERAL + MATERIALIZED with TSV / Native / Buffers

$CLICKHOUSE_CLIENT -n <<SQL
DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    x UInt8 EPHEMERAL,
    s String MATERIALIZED format('Hello {} world', x)
)
ORDER BY ();
SQL

# Insert via TSV
$CLICKHOUSE_LOCAL  -q "SELECT 12 AS x FORMAT TSV"    | $CLICKHOUSE_CLIENT -q "INSERT INTO test (x) FORMAT TSV"
$CLICKHOUSE_LOCAL  -q "SELECT 34 AS x FORMAT TSV"    | $CLICKHOUSE_CLIENT -q "INSERT INTO test (*, x) FORMAT TSV"

# Insert via Native
$CLICKHOUSE_LOCAL  -q "SELECT 56 AS x FORMAT Native" | $CLICKHOUSE_CLIENT -q "INSERT INTO test (x) FORMAT Native"
$CLICKHOUSE_LOCAL  -q "SELECT 78 AS x FORMAT Native" | $CLICKHOUSE_CLIENT -q "INSERT INTO test (*, x) FORMAT Native"

# Insert via Buffers
$CLICKHOUSE_LOCAL  -q "SELECT 90  AS x FORMAT Buffers"  | $CLICKHOUSE_CLIENT -q "INSERT INTO test (x) FORMAT Buffers"
$CLICKHOUSE_LOCAL  -q "SELECT 123 AS x FORMAT Buffers"  | $CLICKHOUSE_CLIENT -q "INSERT INTO test (*, x) FORMAT Buffers"

# Check the final contents
$CLICKHOUSE_CLIENT -q "
SELECT 'EPHEMERAL + MATERIALIZED with Buffers / TSV / Native';
SELECT s
FROM test
ORDER BY s
FORMAT TSV;
"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS file_buffers_simple;
DROP TABLE IF EXISTS file_buffers_simple_clone;
DROP TABLE IF EXISTS file_buffers_two_cols;
"

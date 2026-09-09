#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
DROP FUNCTION IF EXISTS wasm_batch_rows;
DELETE FROM system.webassembly_modules WHERE name = 'batch_rows_abi';
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'batch_rows_abi', code FROM input('code String') FORMAT RawBlob" < "${CUR_DIR}"/wasm/buffered_abi.wasm

# `get_block_size` reports, once per row, how many rows the host put in the batch that row belonged to,
# so it is the only observable of the input splitter.
#
# How many rows a batch holds follows from the width of those rows and the byte budget alone. It must
# not follow from what an earlier block, or an earlier query, happened to measure: a row count fitted
# by narrow rows says nothing about wide ones. The wide-row query therefore runs twice, the second time
# after a narrow-row query has gone through the same function object, and both runs must agree.

${CLICKHOUSE_CLIENT} --query "

SET webassembly_udf_max_fuel = 100000000;
SET max_threads = 1;
SET max_block_size = 1000;
-- Split by size rather than by a fixed row count, against a budget small enough to split a block.
SET webassembly_udf_max_input_block_size = 0;
SET webassembly_udf_input_split_memory_ratio = 0.0001;

CREATE OR REPLACE FUNCTION wasm_batch_rows
    LANGUAGE WASM ABI BUFFERED_V1 FROM 'batch_rows_abi' :: 'get_block_size'
    ARGUMENTS (value String) RETURNS UInt64
    SETTINGS serialization_format = 'CSV';

DROP TABLE IF EXISTS batch_sizes;
CREATE TABLE batch_sizes (tag String, run UInt8, min_rows UInt64, max_rows UInt64) ENGINE = Memory;

INSERT INTO batch_sizes SELECT 'wide', 1, min(v), max(v) FROM (SELECT wasm_batch_rows(s) AS v FROM (SELECT repeat('a', 50) AS s FROM numbers(3000)));
INSERT INTO batch_sizes SELECT 'narrow', 1, min(v), max(v) FROM (SELECT wasm_batch_rows(s) AS v FROM (SELECT repeat('a', 1) AS s FROM numbers(3000)));
INSERT INTO batch_sizes SELECT 'wide', 2, min(v), max(v) FROM (SELECT wasm_batch_rows(s) AS v FROM (SELECT repeat('a', 50) AS s FROM numbers(3000)));

SELECT 'wide rows batch smaller than narrow ones',
    (SELECT max(max_rows) FROM batch_sizes WHERE tag = 'wide') < (SELECT min(min_rows) FROM batch_sizes WHERE tag = 'narrow');

SELECT 'wide batches unchanged by a preceding narrow query',
    (SELECT min_rows FROM batch_sizes WHERE tag = 'wide' AND run = 1) = (SELECT min_rows FROM batch_sizes WHERE tag = 'wide' AND run = 2)
    AND (SELECT max_rows FROM batch_sizes WHERE tag = 'wide' AND run = 1) = (SELECT max_rows FROM batch_sizes WHERE tag = 'wide' AND run = 2);

SELECT 'no batch spans more than one block', (SELECT max(max_rows) FROM batch_sizes) <= 1000;

-- A block that fits the budget whole is passed whole, whatever the queries before it measured.
SELECT 'short block passed whole', min(wasm_batch_rows(s) AS v) = 4 AND max(v) = 4 FROM (SELECT repeat('a', 50) AS s FROM numbers(4));

DROP TABLE batch_sizes;
DROP FUNCTION wasm_batch_rows;
"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = 'batch_rows_abi'"

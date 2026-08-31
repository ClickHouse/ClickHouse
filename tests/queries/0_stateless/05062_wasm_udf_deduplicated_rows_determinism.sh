#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MODULE="host_api_${CLICKHOUSE_DATABASE}"
FUNCTION="wasm_random_${CLICKHOUSE_DATABASE}"
FUNCTION_DETERMINISTIC="wasm_random_deterministic_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FUNCTION}"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FUNCTION_DETERMINISTIC}"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"

${CLICKHOUSE_CLIENT} --enable_analyzer 1 \
    -q "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}"/wasm/host_api.wasm

${CLICKHOUSE_CLIENT} --enable_analyzer 1 --allow_suspicious_low_cardinality_types 1 -m -q "
CREATE FUNCTION ${FUNCTION} LANGUAGE WASM ABI ROW_DIRECT FROM '${MODULE}' :: 'test_random' ARGUMENTS (UInt32) RETURNS UInt32;
CREATE FUNCTION ${FUNCTION_DETERMINISTIC} LANGUAGE WASM ABI ROW_DIRECT FROM '${MODULE}' :: 'test_random' ARGUMENTS (UInt32) RETURNS UInt32 DETERMINISTIC;

CREATE TABLE sparse_argument (v UInt32) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;
INSERT INTO sparse_argument SELECT if(number < 95, 0, number) FROM numbers(100);

CREATE TABLE low_cardinality_argument (v LowCardinality(UInt32)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO low_cardinality_argument SELECT number % 2 FROM numbers(100);

CREATE TABLE full_argument (v UInt32) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO full_argument SELECT number % 2 FROM numbers(100);

SELECT 'sparse argument is stored sparse', countIf(serialization_kind = 'Sparse') > 0
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'sparse_argument' AND column = 'v' AND active;
SELECT 'control argument is stored full', countIf(serialization_kind = 'Sparse') = 0
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'full_argument' AND column = 'v' AND active;

SELECT 'distinct results over sparse argument', uniqExact(${FUNCTION}(v)) > 50 FROM sparse_argument;
SELECT 'distinct results over low cardinality argument', uniqExact(${FUNCTION}(v)) > 50 FROM low_cardinality_argument;
SELECT 'distinct results over full argument', uniqExact(${FUNCTION}(v)) > 50 FROM full_argument;

SELECT 'deduplicated over sparse argument when declared deterministic', uniqExact(${FUNCTION_DETERMINISTIC}(v)) <= 10 FROM sparse_argument;
SELECT 'deduplicated over low cardinality argument when declared deterministic', uniqExact(${FUNCTION_DETERMINISTIC}(v)) <= 10 FROM low_cardinality_argument;
"

${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FUNCTION}"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FUNCTION_DETERMINISTIC}"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"

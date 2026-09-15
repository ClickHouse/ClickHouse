#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Tag no-fasttest: needs the WebAssembly runtime
# Tag no-parallel: messes with the query condition cache

# A `LANGUAGE WASM` function declared `DETERMINISTIC` is admitted to the query condition cache, and
# `FunctionUserDefinedWasm` snapshots its execution budget (`webassembly_udf_max_fuel`, `webassembly_udf_max_memory`,
# `webassembly_udf_max_input_block_size`, `webassembly_udf_input_split_memory_ratio`) without leaving a trace in the
# condition's `ActionsDAG`. A "no marks match" verdict primed under a lenient budget must not be served to a session
# whose stricter budget is supposed to raise an exception on the same rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << 'EOF2'
DROP FUNCTION IF EXISTS identity_qcc_det;
DELETE FROM system.webassembly_modules WHERE name = 'identity_qcc_test';
DROP TABLE IF EXISTS t_qcc_wasm;
EOF2

${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_qcc_test', code FROM input('code String') FORMAT RawBlob" < "${CUR_DIR}/wasm/identity_int.wasm"

${CLICKHOUSE_CLIENT} << 'EOF2'
-- The query condition cache is only used with the analyzer.
SET enable_analyzer = 1;
SET use_query_condition_cache = 1;
-- Without a local plan the filter steps run as part of the remote queries, and this server's cache sees nothing.
SET parallel_replicas_local_plan = 1;

CREATE OR REPLACE FUNCTION identity_qcc_det
    LANGUAGE WASM FROM 'identity_qcc_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1
    DETERMINISTIC;

-- The auto minmax indexes would answer before the cache, and the cache stores nothing for small parts, so the
-- granularity is small to still span several marks with few rows (a guest call per block is slow under the sanitizers).
CREATE TABLE t_qcc_wasm (k UInt64, x Int32) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 128, add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
INSERT INTO t_qcc_wasm SELECT number, number % 1000 FROM numbers(8192);

-- Prime the cache with an unlimited budget: the identity function never returns -1, so no mark matches.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_wasm WHERE identity_qcc_det(x) = -1 SETTINGS webassembly_udf_max_fuel = 0, webassembly_udf_max_memory = 134217728, webassembly_udf_input_split_memory_ratio = 0.5;

-- A stricter budget on the same predicate must raise instead of being served the cached verdict:
-- one unit of fuel is exhausted by the first call,
SELECT count() FROM t_qcc_wasm WHERE identity_qcc_det(x) = -1 SETTINGS webassembly_udf_max_fuel = 1; -- { serverError WASM_ERROR }
-- a memory limit below the module's minimum memory cannot even instantiate it,
SELECT count() FROM t_qcc_wasm WHERE identity_qcc_det(x) = -1 SETTINGS webassembly_udf_max_memory = 1024; -- { serverError WASM_ERROR }
-- and a ratio above 1 is rejected where the batch size is decided.
SELECT count() FROM t_qcc_wasm WHERE identity_qcc_det(x) = -1 SETTINGS webassembly_udf_input_split_memory_ratio = 2; -- { serverError BAD_ARGUMENTS }

-- The verdict itself is still valid for the budget it was primed under.
SELECT count() FROM t_qcc_wasm WHERE identity_qcc_det(x) = -1 SETTINGS webassembly_udf_max_fuel = 0, webassembly_udf_max_memory = 134217728, webassembly_udf_input_split_memory_ratio = 0.5;
SELECT count() FROM t_qcc_wasm WHERE identity_qcc_det(x) = -1 SETTINGS use_query_condition_cache = 0;

DROP TABLE t_qcc_wasm;
DROP FUNCTION identity_qcc_det;
DELETE FROM system.webassembly_modules WHERE name = 'identity_qcc_test';
EOF2

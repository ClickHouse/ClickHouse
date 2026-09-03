#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for the BUFFERED_V1 dynamic splitter's size estimate with a declared
# fixed-width Nullable argument. isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion()
# returns false for DataTypeNullable itself (only true for the nested type), so sizing by the
# still-Nullable declared_type falls through to a flat 256-byte-per-row fallback instead of the
# real ~2 bytes/row (1 data byte + 1 null-map byte) of a Nullable(Int8) column. That inflates the
# splitter's estimate by ~100x, forcing far smaller batches than the guest's real (tiny, 1 MiB)
# heap requires.
#
# RETURNS Nullable(UInt32) is deliberate: a WASM UDF owns null propagation exactly when it
# declares a Nullable return type, so ClickHouse disables its default null-handling for this
# function and passes a genuinely-Nullable argument column through to executeImpl -- a plain
# UInt32 return would have ClickHouse strip Nullable from the argument before the call, never
# reaching the buggy code path at all.
#
# The guest returns, for every input row, the number of rows in the batch it arrived in, so SQL
# can assert both that every row was processed and that batches stayed reasonably large instead
# of collapsing to near-single-row batches under the inflated estimate.

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS wasm_csv_batch_rows_nullable;
DELETE FROM system.webassembly_modules WHERE name = 'text_split_abi_nullable';
EOF

cat "${CUR_DIR}/wasm/text_split_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'text_split_abi_nullable', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
CREATE OR REPLACE FUNCTION wasm_csv_batch_rows_nullable
    LANGUAGE WASM ABI BUFFERED_V1
    FROM 'text_split_abi_nullable' :: 'batch_row_count_json_scalar'
    ARGUMENTS (x Nullable(Int8)) RETURNS Nullable(UInt32)
    SETTINGS serialization_format = 'JSONEachRow';

-- A single 262144-row block of Nullable(Int8) values (some NULL): well within the guest's
-- real linear-memory budget once sized correctly (~2 bytes/row), so it should split into a
-- handful of large batches, not near-single-row batches as the flat 256-byte fallback would force.
SELECT count() = 262144 AS all_rows_processed,
       max(batch_rows) < 262144 AS was_split,
       min(batch_rows) > 1000 AS batches_not_collapsed
FROM
(
    SELECT wasm_csv_batch_rows_nullable(if(number % 10 = 0, NULL, toInt8((number % 2) - 100))) AS batch_rows
    FROM numbers(262144)
)
SETTINGS max_block_size = 262144, max_threads = 1, webassembly_udf_max_input_block_size = 0;

DROP FUNCTION IF EXISTS wasm_csv_batch_rows_nullable;
DELETE FROM system.webassembly_modules WHERE name = 'text_split_abi_nullable';
EOF

#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# no-fasttest: needs the SZ3 library
# no-shared-merge-tree: the test uses `SYSTEM STOP MERGES` to keep a mutation of a plain local
# MergeTree table queued and polls `system.mutations` of the single local server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The lossy-codec dependency guard of `ALTER TABLE ... RECOMPRESS COLUMN` runs when the ALTER is
# accepted, but the recompression resolves the codec from the table metadata again when the
# mutation executes. A `MODIFY COLUMN ... CODEC(...)` executed while the mutation is queued can
# therefore invalidate the validation after the fact: `RECOMPRESS COLUMN val` queued while `val`
# is lossless, then `MODIFY COLUMN val CODEC(SZ3(...))` before the mutation runs, would rewrite
# `val` lossily while a dependent skip index keeps describing the old values. The guard is re-run
# at execution time, so such a mutation must fail instead of rewriting the data, and must proceed
# once the conflicting metadata is reverted.

table="t_recompress_lossy_codec_change"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $table"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $table
    (
        key UInt64,
        val Float64 CODEC(ZSTD(1)),
        INDEX idx val TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             max_postpone_time_for_failed_mutations_ms = 100"

${CLICKHOUSE_CLIENT} --query "INSERT INTO $table SELECT number, sin(number / 100.) * 100 FROM numbers(1000)"

# Mutations of a plain MergeTree table are gated on the merges blocker, so the RECOMPRESS stays
# queued until SYSTEM START MERGES.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES $table"

${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE $table RECOMPRESS COLUMN val"

# Metadata-only change; makes the queued recompression lossy.
${CLICKHOUSE_CLIENT} --allow_experimental_codecs 1 --query \
    "ALTER TABLE $table MODIFY COLUMN val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES $table"

# The mutation must fail with the dependency error, not rewrite the data.
fail_reason=""
for _ in {1..600}
do
    fail_reason=$(${CLICKHOUSE_CLIENT} --query "
        SELECT latest_fail_reason FROM system.mutations
        WHERE database = currentDatabase() AND table = '$table' AND NOT is_done")
    if [[ "$fail_reason" == *"idx"* ]]; then break; fi
    sleep 0.3
done

echo "$fail_reason" | grep -o "Cannot RECOMPRESS COLUMN \`val\` with the lossy codec" | head -1
echo "$fail_reason" | grep -o "index \`idx\` depends on this column" | head -1

# The stored values are still the original ones: the lossy rewrite did not run.
${CLICKHOUSE_CLIENT} --query "SELECT countIf(val = sin(key / 100.) * 100) FROM $table"

# Reverting the conflicting codec lets the queued mutation proceed on a retry.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table MODIFY COLUMN val Float64 CODEC(ZSTD(2))"

for _ in {1..600}
do
    pending=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = '$table' AND NOT is_done")
    if [[ "$pending" == "0" ]]; then break; fi
    sleep 0.3
done

${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.mutations
    WHERE database = currentDatabase() AND table = '$table' AND NOT is_done"
${CLICKHOUSE_CLIENT} --query "SELECT countIf(val = sin(key / 100.) * 100) FROM $table"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"

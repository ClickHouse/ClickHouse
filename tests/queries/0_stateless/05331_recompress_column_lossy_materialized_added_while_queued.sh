#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# no-fasttest: needs the SZ3 library
# no-shared-merge-tree: the test uses `SYSTEM STOP MERGES` to keep a mutation of a plain local
# MergeTree table queued and polls `system.mutations` of the single local server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The lossy-codec dependency guard of `ALTER TABLE ... RECOMPRESS COLUMN` is re-run when the
# mutation executes, but only against the `MATERIALIZED` columns the mutated part stores.
# An `ADD COLUMN ... MATERIALIZED` executed while the mutation is queued is metadata-only: the
# older parts the mutation rewrites do not store the new column, so it cannot go stale and must
# not make the queued recompression fail forever.

table="t_recompress_lossy_materialized_added"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $table"
${CLICKHOUSE_CLIENT} --enable_sz3_codec 1 --query "
    CREATE TABLE $table
    (
        key UInt64,
        val Float64 CODEC(ZSTD(1))
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             max_postpone_time_for_failed_mutations_ms = 100"

${CLICKHOUSE_CLIENT} --query "INSERT INTO $table SELECT number, sin(number / 100.) * 100 FROM numbers(1000)"

${CLICKHOUSE_CLIENT} --enable_sz3_codec 1 --query \
    "ALTER TABLE $table MODIFY COLUMN val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))"

# Mutations of a plain MergeTree table are gated on the merges blocker, so the RECOMPRESS stays
# queued until SYSTEM START MERGES.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES $table"

${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE $table RECOMPRESS COLUMN val"

# Metadata-only: the existing part does not store the new column.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table ADD COLUMN m Float64 MATERIALIZED val * 2"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES $table"

for _ in {1..600}
do
    pending=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = '$table' AND NOT is_done")
    if [[ "$pending" == "0" ]]; then break; fi
    sleep 0.3
done

${CLICKHOUSE_CLIENT} --query "
    SELECT count(), max(latest_fail_reason) FROM system.mutations
    WHERE database = currentDatabase() AND table = '$table' AND NOT is_done"

# The column was recompressed lossily: the values moved, but stayed within the error bound.
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), countIf(val = sin(key / 100.) * 100) < 1000, max(abs(val - sin(key / 100.) * 100)) <= 0.01
    FROM $table"

# The column absent from the part is computed from the recompressed values.
${CLICKHOUSE_CLIENT} --query "SELECT countIf(m = val * 2) FROM $table"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"

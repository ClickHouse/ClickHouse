#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# no-fasttest: needs the SZ3 library
# no-shared-merge-tree: the test polls `system.mutations` of the single local server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A part that cannot be recompressed in place (here: a compact part) is rewritten as a whole by
# `ALTER TABLE ... RECOMPRESS COLUMN`, and the rewrite re-serializes every stored column with its
# current codec, not only the recompression target. A metadata-only `MODIFY COLUMN y CODEC(SZ3(...))`
# on another column would therefore piggyback on `RECOMPRESS COLUMN x` and rewrite `y` lossily even
# though the lossy-codec dependency guard never ran for `y`. The guard must cover every column such
# a rewrite re-serializes: the mutation must fail instead of rewriting `y`, whose dependent skip
# index would keep describing the pre-rewrite values.

table="t_recompress_lossy_non_target"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $table"
# Large `min_bytes_for_wide_part` keeps the part compact, so the recompression takes the
# whole-part rewrite path.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $table
    (
        key UInt64,
        x String CODEC(LZ4),
        y Float64 CODEC(ZSTD(1)),
        INDEX idx y TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
             max_postpone_time_for_failed_mutations_ms = 100"

${CLICKHOUSE_CLIENT} --query "INSERT INTO $table SELECT number, toString(number), sin(number / 100.) * 100 FROM numbers(1000)"

${CLICKHOUSE_CLIENT} --query "SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active"

# Metadata-only change: `y` now has a lossy codec, but its stored data is untouched.
${CLICKHOUSE_CLIENT} --allow_experimental_codecs 1 --query \
    "ALTER TABLE $table MODIFY COLUMN y Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))"

# Recompressing `x` (a lossless target, accepted by the ALTER-time guard) must not silently
# rewrite `y` with the lossy codec.
${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE $table RECOMPRESS COLUMN x"

fail_reason=""
for _ in {1..600}
do
    fail_reason=$(${CLICKHOUSE_CLIENT} --query "
        SELECT latest_fail_reason FROM system.mutations
        WHERE database = currentDatabase() AND table = '$table' AND NOT is_done")
    if [[ "$fail_reason" == *"idx"* ]]; then break; fi
    sleep 0.3
done

echo "$fail_reason" | grep -o "Cannot RECOMPRESS COLUMN \`y\` with the lossy codec" | head -1
echo "$fail_reason" | grep -o "index \`idx\` depends on this column" | head -1
echo "$fail_reason" | grep -o "re-serializes every stored column, including \`y\`" | head -1

# The stored values are still the original ones: the lossy rewrite did not run.
${CLICKHOUSE_CLIENT} --query "SELECT countIf(y = sin(key / 100.) * 100) FROM $table"

# Reverting the conflicting codec lets the queued mutation proceed on a retry.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table MODIFY COLUMN y Float64 CODEC(ZSTD(2))"

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
${CLICKHOUSE_CLIENT} --query "SELECT countIf(y = sin(key / 100.) * 100) FROM $table"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"

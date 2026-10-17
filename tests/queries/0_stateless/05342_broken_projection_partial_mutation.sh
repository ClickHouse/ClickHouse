#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation that rewrites only some columns of a Wide part rebuilds a broken projection from the part's real data.
# Projection `q` makes the mutation read `id`, so the block holds some, but not all, of the columns of `p`.

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${data_path:?}"

$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
CREATE TABLE t_plain (id UInt64, g UInt64, w UInt64,
    PROJECTION p (SELECT id, g ORDER BY g), PROJECTION q (SELECT id, w ORDER BY w))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
CREATE TABLE t_commit_order (id UInt64, g UInt64, w UInt64,
    PROJECTION p (SELECT id, g, _block_number, _block_offset ORDER BY _block_number, _block_offset),
    PROJECTION q (SELECT id, w ORDER BY w))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_plain SELECT number + 100, number % 7 + 50, number FROM numbers(10);
INSERT INTO t_commit_order SELECT number + 100, number % 7 + 50, number FROM numbers(10);
OPTIMIZE TABLE t_commit_order FINAL;
"

for table in t_plain t_commit_order; do
    projection_path=$($CLICKHOUSE_LOCAL --path "$data_path" -q "
        SELECT path FROM system.projection_parts WHERE database = currentDatabase() AND table = '$table' AND name = 'p' AND active")
    rm "${projection_path:?}"*.bin
done

$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
SELECT table, is_broken FROM system.projection_parts WHERE database = currentDatabase() AND name = 'p' AND active ORDER BY table;
ALTER TABLE t_plain UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_commit_order UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT table, is_broken, rows FROM system.projection_parts WHERE database = currentDatabase() AND name = 'p' AND active ORDER BY table;
SELECT (SELECT arraySort(groupArray((id, g))) FROM mergeTreeProjection(currentDatabase(), t_plain, p))
    = (SELECT arraySort(groupArray((id, g))) FROM t_plain SETTINGS optimize_use_projections = 0);
SELECT (SELECT arraySort(groupArray((id, g, _block_number, _block_offset))) FROM mergeTreeProjection(currentDatabase(), t_commit_order, p))
    = (SELECT arraySort(groupArray((id, g, _block_number, _block_offset))) FROM t_commit_order SETTINGS optimize_use_projections = 0);
"

rm -rf "${data_path:?}"

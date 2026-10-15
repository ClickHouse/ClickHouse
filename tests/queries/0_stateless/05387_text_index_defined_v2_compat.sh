#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS text_index_defined_v2_compat;

CREATE TABLE text_index_defined_v2_compat
(
    id UInt32,
    value Nullable(String),
    INDEX idx value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 8,
    index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    text_index_serialization_version = 'v2_with_positions';

SYSTEM STOP MERGES text_index_defined_v2_compat;

INSERT INTO text_index_defined_v2_compat VALUES
    (0, 'alpha'), (1, NULL), (2, ''), (3, 'alpha'),
    (4, 'alpha'), (5, NULL), (6, ''), (7, 'alpha'),
    (8, NULL), (9, NULL), (10, ''), (11, 'beta');
"

echo '-- pinned v2 writes no identity streams'
part_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'text_index_defined_v2_compat' AND active")
if [ -f "${part_path}skp_idx_idx.idt.dct.idx" ] || [ -f "${part_path}skp_idx_idx.idt.pst.idx" ]; then
    echo 'unexpected idt files'
    ls "$part_path"
else
    echo 'idt files absent'
fi

echo '-- V2 leftover Exact / count match scan'
$CLICKHOUSE_CLIENT -m -q "
SELECT 'isNull indexed', id FROM text_index_defined_v2_compat WHERE isNull(value) ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1, query_plan_optimize_count_from_text_index = 1;
SELECT 'isNull scan', id FROM text_index_defined_v2_compat WHERE isNull(value) ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0, query_plan_optimize_count_from_text_index = 0;
SELECT 'isNotNull indexed', id FROM text_index_defined_v2_compat WHERE isNotNull(value) ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1, query_plan_optimize_count_from_text_index = 1;
SELECT 'isNotNull scan', id FROM text_index_defined_v2_compat WHERE isNotNull(value) ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0, query_plan_optimize_count_from_text_index = 0;
SELECT 'not hasToken indexed', id FROM text_index_defined_v2_compat WHERE NOT hasToken(value, 'alpha') ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1;
SELECT 'not hasToken scan', id FROM text_index_defined_v2_compat WHERE NOT hasToken(value, 'alpha') ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0;
SELECT 'count isNull indexed', count() FROM text_index_defined_v2_compat WHERE isNull(value)
SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1, query_plan_optimize_count_from_text_index = 1;
SELECT 'count isNull scan', count() FROM text_index_defined_v2_compat WHERE isNull(value)
SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0, query_plan_optimize_count_from_text_index = 0;
SELECT 'count isNotNull indexed', count() FROM text_index_defined_v2_compat WHERE isNotNull(value)
SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1, query_plan_optimize_count_from_text_index = 1;
SELECT 'count isNotNull scan', count() FROM text_index_defined_v2_compat WHERE isNotNull(value)
SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0, query_plan_optimize_count_from_text_index = 0;
"

$CLICKHOUSE_CLIENT -m -q "
ALTER TABLE text_index_defined_v2_compat MODIFY SETTING text_index_serialization_version = 'v3_with_defined';
INSERT INTO text_index_defined_v2_compat VALUES
    (12, 'alpha'), (13, NULL), (14, ''), (15, 'beta');
"

echo '-- mixed V2 leftover + V3 part still matches scan'
$CLICKHOUSE_CLIENT -m -q "
SELECT 'mixed isNull indexed', id FROM text_index_defined_v2_compat WHERE isNull(value) ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1, query_plan_optimize_count_from_text_index = 1;
SELECT 'mixed isNull scan', id FROM text_index_defined_v2_compat WHERE isNull(value) ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0, query_plan_optimize_count_from_text_index = 0;
SELECT 'mixed count isNull indexed', count() FROM text_index_defined_v2_compat WHERE isNull(value)
SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1, query_plan_optimize_count_from_text_index = 1;
SELECT 'mixed count isNull scan', count() FROM text_index_defined_v2_compat WHERE isNull(value)
SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0, query_plan_optimize_count_from_text_index = 0;
SELECT 'mixed not hasToken indexed', id FROM text_index_defined_v2_compat WHERE NOT hasToken(value, 'alpha') ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1;
SELECT 'mixed not hasToken scan', id FROM text_index_defined_v2_compat WHERE NOT hasToken(value, 'alpha') ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0;
"

echo '-- new part after unpin has identity streams'
v3_parts=$($CLICKHOUSE_CLIENT -q "
SELECT path FROM system.parts
WHERE database = currentDatabase() AND table = 'text_index_defined_v2_compat' AND active
ORDER BY min_block_number
")
idt_parts=0
total_parts=0
while IFS= read -r path; do
    [ -z "$path" ] && continue
    total_parts=$((total_parts + 1))
    if [ -f "${path}skp_idx_idx.idt.dct.idx" ] && [ -f "${path}skp_idx_idx.idt.pst.idx" ]; then
        idt_parts=$((idt_parts + 1))
    fi
done <<< "$v3_parts"
echo "parts with identity: ${idt_parts}/${total_parts}"

$CLICKHOUSE_CLIENT -q "DROP TABLE text_index_defined_v2_compat;"

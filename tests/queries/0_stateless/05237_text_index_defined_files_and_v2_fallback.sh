#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS text_index_defined_files;
DROP TABLE IF EXISTS text_index_defined_plain;
DROP TABLE IF EXISTS text_index_defined_v2_fallback;

CREATE TABLE text_index_defined_files
(
    id UInt32,
    value Nullable(String),
    INDEX idx value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES text_index_defined_files;
INSERT INTO text_index_defined_files VALUES
    (0, 'alpha'), (1, NULL), (2, ''), (3, 'alpha'),
    (4, 'alpha'), (5, NULL), (6, ''), (7, 'alpha'),
    (8, NULL), (9, NULL), (10, ''), (11, 'beta'),
    (12, 'alpha'), (13, NULL), (14, ''), (15, 'beta'),
    (16, 'alpha');

CREATE TABLE text_index_defined_plain
(
    id UInt32,
    value String,
    INDEX idx value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES text_index_defined_plain;
INSERT INTO text_index_defined_plain VALUES
    (0, 'alpha'), (1, ''), (2, 'beta'), (3, 'alpha'),
    (4, 'alpha'), (5, ''), (6, 'beta'), (7, 'alpha'),
    (8, 'x'), (9, 'y'), (10, ''), (11, 'beta'),
    (12, 'alpha'), (13, 'z'), (14, ''), (15, 'beta'),
    (16, 'alpha');

CREATE TABLE text_index_defined_v2_fallback
(
    id UInt32,
    value Nullable(String),
    INDEX idx value TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(value))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES text_index_defined_v2_fallback;
INSERT INTO text_index_defined_v2_fallback VALUES
    (0, 'Alpha'), (1, NULL), (2, ''), (3, 'alpha'),
    (4, 'Alpha'), (5, NULL), (6, ''), (7, 'alpha'),
    (8, NULL), (9, NULL), (10, ''), (11, 'Beta'),
    (12, 'Alpha'), (13, NULL), (14, ''), (15, 'Beta'),
    (16, 'alpha');
"

echo '-- nullable V3 has identity streams'
part_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'text_index_defined_files' AND active")
if [ -f "${part_path}skp_idx_idx.idt.dct.idx" ] && [ -f "${part_path}skp_idx_idx.idt.pst.idx" ]; then
    echo 'idt files exist'
else
    echo "missing idt files in $part_path:"
    ls "$part_path"
fi

echo '-- non-nullable V2 has no identity streams'
plain_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'text_index_defined_plain' AND active")
if [ -f "${plain_path}skp_idx_idx.idt.dct.idx" ] || [ -f "${plain_path}skp_idx_idx.idt.pst.idx" ]; then
    echo 'unexpected idt files'
    ls "$plain_path"
else
    echo 'idt files absent'
fi

echo '-- preprocessor nullable part has no identity streams'
fallback_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'text_index_defined_v2_fallback' AND active")
if [ -f "${fallback_path}skp_idx_idx.idt.dct.idx" ] || [ -f "${fallback_path}skp_idx_idx.idt.pst.idx" ]; then
    echo 'unexpected idt files'
    ls "$fallback_path"
else
    echo 'idt files absent'
fi

echo '-- V2 fallback NOT hasToken matches scan'
$CLICKHOUSE_CLIENT -m -q "
SELECT id FROM text_index_defined_v2_fallback WHERE NOT hasToken(value, 'alpha') ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1;
SELECT id FROM text_index_defined_v2_fallback WHERE NOT hasToken(value, 'alpha') ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0;
"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE text_index_defined_files;
DROP TABLE text_index_defined_plain;
DROP TABLE text_index_defined_v2_fallback;
"

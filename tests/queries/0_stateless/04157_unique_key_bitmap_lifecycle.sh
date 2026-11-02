#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# UNIQUE KEY: a DELETE's bitmap lives in the 0-row marker it publishes and survives a reload.
#   1. marker: its shape, and a read after reload with no warm-up
#   2. two partitions: the DELETE reads the same after DETACH/ATTACH, and another DELETE works

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_unique_key 1"

# 1. marker: red if the read path requires warmed-up in-memory state (`survivors_after_reload`).
# `marker_shape` and `bitmap_versions_on_data_part` are preconditions.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS uk_marker"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE uk_marker (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES uk_marker"
$CLICKHOUSE_CLIENT --query "INSERT INTO uk_marker VALUES (10, 'a'), (20, 'b'), (30, 'c')"
$CLICKHOUSE_CLIENT --query "DELETE FROM uk_marker WHERE id = 20"

$CLICKHOUSE_CLIENT --query "
    SELECT 'marker_shape', name, rows, level, unique_key_bitmap_versions FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_marker' AND active AND rows = 0"

$CLICKHOUSE_CLIENT --query "
    SELECT 'bitmap_versions_on_data_part', sum(length(unique_key_bitmap_versions)) FROM system.parts
    WHERE database = currentDatabase() AND table = 'uk_marker' AND active AND rows > 0"

$CLICKHOUSE_CLIENT --query "DETACH TABLE uk_marker"
$CLICKHOUSE_CLIENT --query "ATTACH TABLE uk_marker"

$CLICKHOUSE_CLIENT --query "
    SELECT 'survivors_after_reload', groupArray(id) FROM (SELECT id FROM uk_marker ORDER BY id)"
$CLICKHOUSE_CLIENT --query "
    SELECT 'live_count_after_reload', count() FROM uk_marker
    SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0"

$CLICKHOUSE_CLIENT --query "DROP TABLE uk_marker"

# 2. two partitions: red if the table does not load its bitmaps on ATTACH (`after_count` 6).
$CLICKHOUSE_CLIENT --multiquery <<'SQL'
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS uk_reload;

CREATE TABLE uk_reload (id UInt64, p UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
PARTITION BY p
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES uk_reload;

INSERT INTO uk_reload VALUES
    (1, 10, 'a'), (2, 10, 'b'), (3, 10, 'c'),
    (4, 20, 'd'), (5, 20, 'e'), (6, 20, 'f');

DELETE FROM uk_reload WHERE id IN (2, 5);

SELECT 'before_count' AS step, count() FROM uk_reload;  -- 4
SELECT 'before_survivors' AS step, id, p, v FROM uk_reload ORDER BY id;  -- 1,3,4,6

DETACH TABLE uk_reload;
ATTACH TABLE uk_reload;

SYSTEM STOP MERGES uk_reload;

SELECT 'after_count' AS step, count() FROM uk_reload;  -- 4
SELECT 'after_survivors' AS step, id, p, v FROM uk_reload ORDER BY id;  -- 1,3,4,6
SELECT 'after_p10' AS step, count() FROM uk_reload WHERE p = 10;  -- 1,3 -> 2
SELECT 'after_p20' AS step, count() FROM uk_reload WHERE p = 20;  -- 4,6 -> 2

DELETE FROM uk_reload WHERE id = 3;
SELECT 'after_more_delete' AS step, count() FROM uk_reload;  -- 3
SELECT 'after_more_survivors' AS step, id FROM uk_reload ORDER BY id;  -- 1,4,6

DROP TABLE uk_reload;
SQL

#!/usr/bin/env bash
# Tags: no-shared-catalog
# no-shared-catalog: STOP MERGES will only stop them only on one replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_lwu_mutations SYNC;
    SET enable_lightweight_update = 1;

    CREATE TABLE t_lwu_mutations (id UInt64, u UInt64, s String)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_mutations/', '1')
    ORDER BY id PARTITION BY id % 2
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        apply_patches_on_merge = 1;

    SYSTEM STOP MERGES t_lwu_mutations;

    INSERT INTO t_lwu_mutations SELECT number, number, 'c' || number FROM numbers(10000);

    UPDATE t_lwu_mutations SET s = s || '_foo' WHERE id % 3 = 0;
    UPDATE t_lwu_mutations SET u = id * 10 WHERE id % 3 = 1; -- (patch 1)

    SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_mutations SETTINGS apply_patch_parts = 0;
    SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_mutations SETTINGS apply_patch_parts = 1;

    -- no patches applied
    SELECT sum(number) FROM numbers(10000);
    -- applied patch 1
    SELECT sum(multiIf(number % 3 = 1, number * 10, number)) FROM numbers(10000);

    ALTER TABLE t_lwu_mutations UPDATE u = 0 WHERE id % 3 = 0 SETTINGS mutations_sync = 0;
    UPDATE t_lwu_mutations SET u = 1000 WHERE id % 3 = 2; -- (patch 2)

    SET apply_mutations_on_fly = 0;

    SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_mutations SETTINGS apply_patch_parts = 0;
    SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_mutations SETTINGS apply_patch_parts = 1;

    -- no patches applied
    SELECT sum(number) FROM numbers(10000);
    -- applied patch 1 and patch 2
    SELECT sum(multiIf(number % 3 = 1, number * 10, number % 3 = 2, 1000, number)) FROM numbers(10000);

    SYSTEM START MERGES t_lwu_mutations;
"

wait_for_mutation "t_lwu_mutations" "0000000000"

$CLICKHOUSE_CLIENT --query "
    SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_mutations SETTINGS apply_patch_parts = 0;
    SELECT sum(u), countIf(endsWith(s, '_foo')) FROM t_lwu_mutations SETTINGS apply_patch_parts = 1;

    -- applied patch 1 and ALTER UPDATE
    SELECT sum(multiIf(number % 3 = 0, 0, number % 3 = 1, number * 10, number)) FROM numbers(10000);
    -- applied patch 1, patch 2 and ALTER UPDATE
    SELECT sum(multiIf(number % 3 = 0, 0, number % 3 = 1, number * 10, number % 3 = 2, 1000, number)) FROM numbers(10000);

    DROP TABLE t_lwu_mutations SYNC;
"

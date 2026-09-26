#!/usr/bin/env bash
# Tags: no-shared-catalog, no-parallel
# no-shared-catalog: STOP MERGES will only stop them only on one replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_lwu_on_fly SYNC;
    SET enable_lightweight_update = 1;

    CREATE TABLE t_lwu_on_fly (id UInt64, v String, s String)
    ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_on_fly/', '1') ORDER BY id
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1;

    SYSTEM STOP MERGES t_lwu_on_fly;

    INSERT INTO t_lwu_on_fly VALUES (1, 'a', 'foo') (2, 'b', 'foo'), (3, 'c', 'foo');

    SET mutations_sync = 0;
    SET apply_patch_parts = 1;
    SET apply_mutations_on_fly = 1;

    ALTER TABLE t_lwu_on_fly UPDATE v = 'd' WHERE id = 1;

    UPDATE t_lwu_on_fly SET s = 'qqq' WHERE v = 'd';
    UPDATE t_lwu_on_fly SET v = 'e' WHERE v = 'd';
    UPDATE t_lwu_on_fly SET s = 'www' WHERE s = 'qqq';

    ALTER TABLE t_lwu_on_fly UPDATE v = 'g' WHERE id = 2;

    UPDATE t_lwu_on_fly SET v = 'f', s = 'rrr' WHERE id = 2;

    SYSTEM SYNC REPLICA t_lwu_on_fly LIGHTWEIGHT;

    SELECT * FROM t_lwu_on_fly ORDER BY id;

    SYSTEM START MERGES t_lwu_on_fly;
"

wait_for_mutation "t_lwu_on_fly" "0000000001"

$CLICKHOUSE_CLIENT --query "
    SELECT * FROM t_lwu_on_fly ORDER BY id;

    OPTIMIZE TABLE t_lwu_on_fly FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT * FROM t_lwu_on_fly ORDER BY id;
    SELECT * FROM t_lwu_on_fly ORDER BY id SETTINGS apply_patch_parts = 0;

    DROP TABLE t_lwu_on_fly SYNC;
"

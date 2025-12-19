#!/usr/bin/env bash

# NOTE: this is a partial copy of the 01683_dist_INSERT_block_structure_mismatch,
# but this test also checks the log messages

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --prefer_localhost_replica=0 -m -q "
    DROP TABLE IF EXISTS tmp_01683;
    DROP TABLE IF EXISTS dist_01683;

    CREATE TABLE tmp_01683 (n Int8) ENGINE=Memory;
    CREATE TABLE dist_01683 (n UInt64) Engine=Distributed(test_cluster_two_shards, currentDatabase(), tmp_01683, n);

    SET distributed_foreground_insert=1;
    INSERT INTO dist_01683 VALUES (1),(2);

    SET distributed_foreground_insert=0;
    -- force log messages from the 'SYSTEM FLUSH DISTRIBUTED' context
    SYSTEM STOP DISTRIBUTED SENDS dist_01683;
    INSERT INTO dist_01683 VALUES (1),(2);
    SYSTEM FLUSH DISTRIBUTED dist_01683;

    -- TODO: cover distributed_background_insert_batch=1

    SELECT * FROM tmp_01683 ORDER BY n;

    DROP TABLE tmp_01683;
    DROP TABLE dist_01683;
" |& sed 's/^.*</</g'

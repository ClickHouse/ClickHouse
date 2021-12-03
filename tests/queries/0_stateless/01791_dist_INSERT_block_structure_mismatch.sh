#!/usr/bin/env bash

# NOTE: this is a partial copy of the 01683_dist_INSERT_block_structure_mismatch,
# but this test also checks the log messages

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --prefer_localhost_replica=0 -nm -q "
    DROP TABLE IF EXISTS tmp_01683;
    DROP TABLE IF EXISTS dist_01683;

    CREATE TABLE tmp_01683 (n Int8) ENGINE=Memory;
    CREATE TABLE dist_01683 (n UInt64) Engine=Distributed(test_cluster_two_shards, currentDatabase(), tmp_01683, n);

    SET insert_distributed_sync=1;
    INSERT INTO dist_01683 VALUES (1),(2);

    SET insert_distributed_sync=0;
    INSERT INTO dist_01683 VALUES (1),(2);
    SYSTEM FLUSH DISTRIBUTED dist_01683;

    -- TODO: cover distributed_directory_monitor_batch_inserts=1

    SELECT * FROM tmp_01683 ORDER BY n;

    DROP TABLE tmp_01683;
    DROP TABLE dist_01683;
" |& sed 's/^.*</</g'

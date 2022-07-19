#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib


${CLICKHOUSE_CLIENT} -n --query="
    DROP TABLE IF EXISTS fetches_r1;
    DROP TABLE IF EXISTS fetches_r2"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE fetches_r1(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/fetches', 'r1') ORDER BY x"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE fetches_r2(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/fetches', 'r2') ORDER BY x \
    SETTINGS prefer_fetch_merged_part_time_threshold=0, \
             prefer_fetch_merged_part_size_threshold=0"

${CLICKHOUSE_CLIENT} -n --query="
    INSERT INTO fetches_r1 VALUES (1);
    INSERT INTO fetches_r1 VALUES (2);
    INSERT INTO fetches_r1 VALUES (3)"

${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA fetches_r2"
${CLICKHOUSE_CLIENT} --query="DETACH TABLE fetches_r2"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE fetches_r1 PARTITION tuple() FINAL" --replication_alter_partitions_sync=0
${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA fetches_r1"

# After attach replica r2 should fetch the merged part from r1.
${CLICKHOUSE_CLIENT} --query="ATTACH TABLE fetches_r2"
${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA fetches_r2"

${CLICKHOUSE_CLIENT} --query="SELECT '*** Check data after fetch of merged part ***'"
${CLICKHOUSE_CLIENT} --query="SELECT _part, * FROM fetches_r2 ORDER BY x"

${CLICKHOUSE_CLIENT} --query="DETACH TABLE fetches_r2"

# Add mutation that doesn't change data.
${CLICKHOUSE_CLIENT} --query="ALTER TABLE fetches_r1 DELETE WHERE x = 0" --replication_alter_partitions_sync=0

wait_for_mutation "fetches_r1" "0000000000"
${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA fetches_r1"

# After attach replica r2 should compare checksums for mutated part and clone the local part.
${CLICKHOUSE_CLIENT} --query="ATTACH TABLE fetches_r2"
${CLICKHOUSE_CLIENT} --query="SYSTEM SYNC REPLICA fetches_r2"

${CLICKHOUSE_CLIENT} --query="SELECT '*** Check data after fetch/clone of mutated part ***'"
${CLICKHOUSE_CLIENT} --query="SELECT _part, * FROM fetches_r2 ORDER BY x"

${CLICKHOUSE_CLIENT} -n --query="
    DROP TABLE fetches_r1;
    DROP TABLE fetches_r2"
